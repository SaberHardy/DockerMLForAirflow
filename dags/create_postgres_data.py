from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import json

default_args = {
    'owner': 'saber',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id="python_dag_operator_v2",
         default_args=default_args,
         description='Small Description',
         start_date=datetime(2025, 5, 10, 2),
         catchup=False,
         schedule_interval=None) as dag:
    pass
# 1:11:02