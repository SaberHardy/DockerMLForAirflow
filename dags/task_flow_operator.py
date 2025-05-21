from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'saber',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


@dag(dag_id="dag_with_taskflow_api",
     default_args=default_args,
     start_date=datetime(2025, 5, 10, 2),
     catchup=False,
     schedule_interval=None)
def hello_from_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Saber",
            "last_name": "hello"
        }

    @task()
    def get_age():
        return 19

    @task()
    def great(first_name, last_name, age):
        print(f"Hello this is {first_name} - {last_name}, i'm {age} yo")

    name_dict = get_name()
    age = get_age()

    great(first_name=name_dict["first_name"],
          last_name=name_dict["last_name"],
          age=age)


greeting_dag = hello_from_etl()
