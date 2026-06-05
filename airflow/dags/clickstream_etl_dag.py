from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_DIR = os.getenv('PROJECT_DIR')

default_args = {
    'owner': 'clickstream',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_etl():
    result = subprocess.run(
        ['python', f'{PROJECT_DIR}/etl/etl_job.py'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"ETL failed: {result.stderr}")

def verify_redshift():
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv('REDSHIFT_HOST'),
        port=os.getenv('REDSHIFT_PORT'),
        dbname=os.getenv('REDSHIFT_DB'),
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM clickstream_sessions")
    count = cur.fetchone()[0]
    print(f"Redshift row count: {count}")
    if count == 0:
        raise Exception("No data in Redshift!")
    conn.close()

with DAG(
    'clickstream_etl',
    default_args=default_args,
    description='Hourly ETL from S3 to Redshift',
    schedule='@hourly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='run_spark_etl',
        python_callable=run_etl,
    )

    verify_task = PythonOperator(
        task_id='verify_redshift',
        python_callable=verify_redshift,
    )

    etl_task >> verify_task
