from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'saber',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id="dag_with_cron_expression",
         default_args=default_args,
         start_date=datetime(2025, 5, 10, 2),
         catchup=False) as dag:
    task1 = BashOperator(task_id='cron_task',
                         bash_command="echo this is new call")

    task1