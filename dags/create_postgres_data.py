from airflow.providers.postgres.operators import PostgresOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'saber',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id="dag_with_postgres_operator",
         default_args=default_args,
         description='Small Description',
         start_date=datetime(2025, 5, 10, 2),
         catchup=False,
         schedule_interval=None) as dag:
    task1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id=""
    )
    task1
