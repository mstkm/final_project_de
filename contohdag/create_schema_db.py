import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="create_schema_db",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = DummyOperator(
        task_id = "start",
        dag = dag
    )

    create_stage_table = MySqlOperator(
        task_id="create_stage_table",
        mysql_conn_id="mysql_conn",
        sql="sql/stage_schema.sql",
    )

    create_destination_tables = PostgresOperator(
        task_id="create_destination_tables",
        postgres_conn_id="postgres_conn",
        sql="sql/destination_schema.sql",
    )

    end = DummyOperator(
        task_id = "end",
        dag = dag
    )

    start >> create_stage_table >> create_destination_tables >> end