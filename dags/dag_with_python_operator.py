from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'owner': 'saber',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

import json


class MultiDimensionalArrayEncoder(json.JSONEncoder):
    def encode(self, obj):
        def hint_tuples(item):
            if isinstance(item, tuple):
                return {'__tuple__': True, 'items': item}
            if isinstance(item, list):
                return [hint_tuples(e) for e in item]
            if isinstance(item, dict):
                return {key: hint_tuples(value) for key, value in item.items()}
            else:
                return item

        return super(MultiDimensionalArrayEncoder, self).encode(hint_tuples(obj))


enc = MultiDimensionalArrayEncoder()
jsonstring = enc.encode([1, 2, (3, 4), [5, 6, (7, 8)]])


def solve_quadratic_equation(a, b, c):
    delta = (b ** 2) - 4 * (a * c)

    import cmath
    if delta > 0:
        x1 = (-b - delta ** 0.5) / (2 * a)
        x2 = (-b + delta ** 0.5) / (2 * a)
    elif delta == 0:
        x1 = (-b - delta ** 0.5) / (2 * a)
        x2 = x1
    else:
        root1 = (-b - cmath.sqrt(delta)) / (2 * a)
        root2 = (-b + cmath.sqrt(delta)) / (2 * a)
        x1 = {"real": root1.real, "imag": root1.imag}
        x2 = {"real": root2.real, "imag": root2.imag}
    print("Json seriazable: ", x1, x2)
    return json.dumps({"x1": x1, "x2": x2})


with DAG(dag_id="python_dag_operator_v2",
         default_args=default_args,
         description='This dag is to calculate the second degree equation',
         start_date=datetime(2025, 5, 10, 2),
         catchup=False,
         schedule_interval=None) as dag:
    task1 = PythonOperator(task_id='great',
                           python_callable=solve_quadratic_equation,
                           op_kwargs={"a": 5, "b": 6, "c": 2})
    task1
