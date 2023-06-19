from datetime import timedelta
from airflow import DAG
from airflow.utils import dates
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import os
import boto3
import psycopg2
import pendulum
import pandas as pd
from datetime import date
from pysurfline import SurfReport

import os
from dotenv import load_dotenv


load_dotenv()
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "AKIAWJWAQWJOIDQDQY3P")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "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")


dag_path = os.getcwd()
print(dag_path)


# functions download and load data
def download_data():
    params = {"spotId": "5842041f4e65fad6a77087f9", "days": 1, "intervalHours": 1}

    today = date.today()
    surfdate = today.strftime("%m-%d-%y")

    report = SurfReport(params)
    print(report.api_log)

    surf_report = report.df.drop(columns=["utcOffset", "swells"])
    surf_report.to_csv(
        dag_path + "/raw_data/" + surfdate + "-surf-report.csv", header=False
    )


def load_s3_data():
    today = date.today()
    surfdate = today.strftime("%m-%d-%y")
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.resource("s3")

    s3.meta.client.upload_file(
        dag_path + "/raw_data/" + surfdate + "-surf-report.csv",
        "surfline-bucket",
        surfdate + "-surf-report.csv",
    )


def download_s3_data():
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.resource("s3")
    bucket = s3.Bucket("surfline-bucket")

    # Iterate over the objects in the bucket
    last_modified_file = None
    last_modified_time = None
    
    for obj in bucket.objects.all():
        if last_modified_time is None or obj.last_modified > last_modified_time:
            last_modified_file = obj.key
            last_modified_time = obj.last_modified
    
    print("Last modified file:", last_modified_file)

    bucket.download_file(
        last_modified_file, dag_path + "/processed_data/" + last_modified_file
    )
    return last_modified_file


def load_data(last_added):

    conn = psycopg2.connect(
        database="surfline",
        user="postgres",
        password="postgres",
        host="db",
        port="5432",
    )

    cursor = conn.cursor()

    cursor.execute("select version()")

    data = cursor.fetchone()
    print("Connection established to: ", data)

    command = """
        CREATE TEMPORARY TABLE IF NOT EXISTS staging_surf_report (
            timestamp TIMESTAMP PRIMARY KEY,
            surf_min INTEGER,
            surf_max INTEGER,
            surf_optimalScore INTEGER,
            surf_plus BOOL,
            surf_humanRelation VARCHAR(255),
            surf_raw_min NUMERIC,
            surf_raw_max NUMERIC,
            speed NUMERIC,
            direction NUMERIC,
            directionType VARCHAR(255),
            gust NUMERIC,
            optimalScore INTEGER,
            temperature NUMERIC,
            condition VARCHAR(255)
        );
        """

    print(command)
    cursor.execute(command)
    conn.commit()

    print(last_added)
    f = open(dag_path + "/processed_data/" + last_added, "r")
    print(f.read())
    try:
        cursor.copy_from(f, "staging_surf_report", sep=",")
        print("Data inserted using copy_from_datafile() successfully....")
    except (Exception, psycopg2.DatabaseError) as err:
        # os.remove(dag_path+"/processed_data/"+last_added)
        # pass exception to function
        print(err)
        print(psycopg2.DatabaseError)
        print(Exception)
        # show_psycopg2_exception(err)
        cursor.close()
    conn.commit()

#     cursor.execute("""select exists(select relname from pg_class 
# where relname = 'surf_report' and relkind='r');""")
    
    # exists = cursor.fetchone()[0]

    command_1 = """
    CREATE TABLE surf_report AS
    SELECT *
        FROM staging_surf_report;
    """
    command_2 = """
        INSERT INTO surf_report
            (timestamp, surf_min, surf_max, surf_optimalScore, surf_plus, 
            surf_humanRelation, surf_raw_min, surf_raw_max, speed, direction, 
            directionType, gust, optimalScore, temperature, condition)
        SELECT *
        FROM staging_surf_report
        WHERE NOT EXISTS(
            SELECT timestamp
            FROM surf_report 
            WHERE staging_surf_report.timestamp = surf_report.timestamp
            );
        """
    # print("exists: ", exists)
    # if exists:
    cursor.execute(command_1)
    # else:
    cursor.execute(command_2)
    conn.commit()
    conn.close()


# default args to dag
default_args = {"owner": "airflow", "start_date": pendulum.now()}

# dag definition
ingestion_dag = DAG(
    "surf_dag",
    default_args=default_args,
    description="Aggregates booking records for data analysis",
    schedule=dates.timedelta(days=1),
    catchup=False,
)

# tasks to run in dag
download_data = PythonOperator(
    task_id="download_data", python_callable=download_data, dag=ingestion_dag,
)

load_s3_data = PythonOperator(
    task_id="load_s3_data", python_callable=load_s3_data, dag=ingestion_dag,
)

download_s3_data_task = PythonOperator(
    task_id="download_s3_data", 
    python_callable=download_s3_data, 
    dag=ingestion_dag,
)

load_data = PythonOperator(
    task_id="load_data", 
    python_callable=load_data, 
    dag=ingestion_dag, 
    op_args=[download_s3_data_task.output],
)

download_data >> load_s3_data >> download_s3_data_task >> load_data