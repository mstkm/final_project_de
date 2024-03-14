from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def test():
    print("Test")

with DAG(
    dag_id="test",
    start_date=datetime(2024, 3, 12),
    schedule_interval="@once",
) as dag:
    task = PythonOperator(
        task_id='test',
        python_callable=test,
    )