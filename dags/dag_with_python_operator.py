from multiprocessing.sharedctypes import typecode_to_type

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


def to_json_meth(str_data):
    import json

    json_acceptable_string = str_data.replace("'", "\"")
    d = json.loads(json_acceptable_string)
    return d


def use_xcom_returned_values(ti):
    variables = ti.xcom_pull(task_ids='great')
    variables = to_json_meth(variables)

    print(f"You have pulled: {variables}, type: {type(variables)}")
    # {'x1': {'real': -0.6, 'imag': -0.2}, 'x2': {'real': -0.6, 'imag': 0.2}}
    real_x1 = variables.get("x1").get("real")
    img_x1 = variables.get("x1").get("imag")

    real_x2 = variables.get("x2").get("real")
    img_x2 = variables.get("x2").get("imag")

    dict_param = {
        "real_x1": real_x1,
        "img_x1": img_x1,
        "real_x2": real_x2,
        "img_x2": img_x2,
    }
    print(f"The dictionary contains: {dict_param}")
    return dict_param


with DAG(dag_id="python_dag_operator_v2",
         default_args=default_args,
         description='This dag is to calculate the second degree equation',
         start_date=datetime(2025, 5, 10, 2),
         catchup=False,
         schedule_interval=None) as dag:
    task1 = PythonOperator(task_id='great',
                           python_callable=solve_quadratic_equation,
                           op_kwargs={"a": 5, "b": 6, "c": 2})

    task2 = PythonOperator(task_id='pull_variables',
                           python_callable=use_xcom_returned_values)
    task1 >> task2
