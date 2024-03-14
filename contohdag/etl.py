from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine

def extract_data_from_api():
    # Step 1: Create a DataFrame with the data
    url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['data']['content'])

    # Step 2: Create a SQLAlchemy engine to connect to the MySQL database
    engine = create_engine("mysql+mysqlconnector://root:secret@172.18.0.1/stage_db")

    # Step 3: Convert the Pandas DataFrame to a format for MySQL table insertion
    df.to_sql('pikobar_staging', con=engine, if_exists='replace', index=False)

    print('OK')


with DAG(
    dag_id="etl_dag",
    start_date=datetime.datetime(2024, 3, 12),
    schedule_interval="@once",
) as dag:
    extract = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api,
    )

    extract