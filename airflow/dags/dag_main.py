from datetime import timedelta, datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from classes.process import Process

process = Process()

default_args = {
    'owner': 'Nagao',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

@dag(
    dag_id='main_dag',
    default_args=default_args,
    description='DAG responsavel pelo ETL do case breweries',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 26),
    catchup=False,
    tags=["api_extraction"]
)
def main_dag():
    api_url = 'https://api.openbrewerydb.org/v1/'
    bronze_bucket = 'bronze'
    silver_bucket = 'silver'
    gold_bucket = 'gold'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'

    extract = PythonOperator(
            task_id='api2bronze',
            python_callable=process.api2bronze,
            op_args=[api_url, bronze_bucket, endpoint_url, access_key, secret_key],
        )
    silver= PythonOperator(
            task_id='bronze2silver',
            python_callable=process.bronze2silver,
            op_args=["", silver_bucket, endpoint_url, access_key, secret_key],
        )
    gold= PythonOperator(
            task_id='silver2gold',
            python_callable=process.silver2gold,
            op_args=["", gold_bucket, endpoint_url, access_key, secret_key],
        )

    validate_and_test= PythonOperator(
            task_id='validate_and_test',
            python_callable=process.validate_and_test,
            op_args=["", endpoint_url, access_key, secret_key],
        )

    extract >> silver >> gold >> validate_and_test

main_dag_instance = main_dag()
