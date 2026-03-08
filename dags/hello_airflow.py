from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet():
    print("Airflow 3 is officially running in WSL!")

with DAG(
    dag_id="01_hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=greet
    )
