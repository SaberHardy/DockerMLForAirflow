from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'saber',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def print_hello():
    print("hello")


with DAG(
        dag_id="first_dag",
        default_args=default_args,
        description='This is our first dag that we write',
        start_date=datetime(2021, 7, 29, 2),
        schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hey, I am task2 and will be running after task1!"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )

    task1 >> [task2, task3]
