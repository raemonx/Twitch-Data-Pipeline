from datetime import timedelta
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from twitch_api_etl import run_twitch_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 07, 17),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'twitch_dag',
    default_args=default_args,
    description='Twtich API data DAG',
    schedule_interval= schedule_interval='*/5 * * * *',
)

run = PythonOperator(
    task_id = 'twitch_etl',
    python_callable = run_twitch_etl,
    dag = dag,
)
