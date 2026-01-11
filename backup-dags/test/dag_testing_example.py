# example_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def transform_data(**context):
    data = context['params']['data']
    return [d.upper() for d in data]

with DAG("dag_testing_example", start_date=datetime(2024,1,1), schedule=None) as dag:
    
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        params={'data': ['apple', 'banana']}
    )

    start_task >> transform >> end_task
