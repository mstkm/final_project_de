from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator
import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from sqlalchemy.engine import URL

def extract_pikobar_staging_table():
    url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian'
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['data']['content'])

    engine = create_engine("mysql+mysqlconnector://root:secret@172.18.0.1/stage_db")
    df.to_sql('pikobar_staging', con=engine, if_exists='replace', index=False)

    print('OK')

def extract_dim_province_table():
    mydb = mysql.connector.connect(
        host="172.18.0.1",
        user="root",
        password="secret",
        database="stage_db"
    )
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT kode_prov, nama_prov FROM pikobar_staging")
    data = mycursor.fetchall()
    arr = []
    for item in data:
        thisdict = {
            "province_id": item[0],
            "province_name": item[1]
        }
        arr.append(thisdict)
    
    engine = create_engine("mysql+mysqlconnector://root:secret@172.18.0.1/stage_db")
    df = pd.DataFrame(arr)
    df.to_sql('dim_province', con=engine, if_exists='replace', index=False)
    print("OK")    

def extract_dim_district_table():
    mydb = mysql.connector.connect(
        host="172.18.0.1",
        user="root",
        password="secret",
        database="stage_db"
    )
    mycursor = mydb.cursor()
    mycursor.execute("SELECT DISTINCT kode_kab, kode_prov, nama_kab FROM pikobar_staging")
    data = mycursor.fetchall()
    arr = []
    for item in data:
        thisdict = {
            "district_id": item[0],
            "province_id": item[1],
            "district_name": item[2]
        }
        arr.append(thisdict)
    
    engine = create_engine("mysql+mysqlconnector://root:secret@172.18.0.1/stage_db")
    df = pd.DataFrame(arr)
    df.to_sql('dim_district', con=engine, if_exists='replace', index=False)
    print("OK")    

def load_district_monthly(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_district_monthly') 
    arr = []
    for item in data:
        thisdict = {
            'district_id': item[0],
            'case_id': item[1],
            'month': item[2],
            'total': item[3],
        }
        arr.append(thisdict)
        
    engine = create_engine('postgresql+psycopg2://postgres:secret@172.18.0.1:5436/destination_db')
    df = pd.DataFrame(arr)
    df.to_sql('district_monthly', con=engine, if_exists='replace', index=False)
    print("Message: Success load district monthly")  

def load_district_yearly(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_district_yearly')
    arr = []
    for item in data:
        thisdict = {
            'district_id': item[0],
            'case_id': item[1],
            'year': item[2],
            'total': item[3],
        }
        arr.append(thisdict)
    
    engine = create_engine('postgresql+psycopg2://postgres:secret@172.18.0.1:5436/destination_db')
    df = pd.DataFrame(arr)
    df.to_sql('district_yearly', con=engine, if_exists='replace', index=False)
    print("Message: Success load district yearly") 

def load_province_daily(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_province_daily')
    arr = []
    for item in data:
        thisdict = {
            'province_id': item[0],
            'case_id': item[1],
            'date': item[2],
            'total': item[3],
        }
        arr.append(thisdict)

    engine = create_engine('postgresql+psycopg2://postgres:secret@172.18.0.1:5436/destination_db')
    df = pd.DataFrame(arr)
    df.to_sql('province_daily', con=engine, if_exists='replace', index=False)
    print("Message: Success load province daily") 

def load_province_monthly(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_province_monthly')
    arr = []
    for item in data:
        thisdict = {
            'province_id': item[0],
            'case_id': item[1],
            'month': item[2],
            'total': item[3],
        }
        arr.append(thisdict)

    engine = create_engine('postgresql+psycopg2://postgres:secret@172.18.0.1:5436/destination_db')
    df = pd.DataFrame(arr)
    df.to_sql('province_monthly', con=engine, if_exists='replace', index=False)
    print("Message: Success load province monthly") 

def load_province_yearly(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_province_yearly')
    arr = []
    for item in data:
        thisdict = {
            'province_id': item[0],
            'case_id': item[1],
            'year': item[2],
            'total': item[3],
        }
        arr.append(thisdict)

    engine = create_engine('postgresql+psycopg2://postgres:secret@172.18.0.1:5436/destination_db')
    df = pd.DataFrame(arr)
    df.to_sql('province_yearly', con=engine, if_exists='replace', index=False)
    print("Message: Success load province yearly") 

with DAG(
    dag_id="etl_dag",
    start_date=datetime.datetime(2024, 3, 12),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(
        task_id = "start",
        dag = dag
    )

    task_1 = PythonOperator(
        task_id='extract_pikobar_staging_table',
        python_callable=extract_pikobar_staging_table,
    )

    task_2 = PythonOperator(
        task_id='extract_dim_province_table',
        python_callable=extract_dim_province_table,
    )

    task_3 = PythonOperator(
        task_id='extract_dim_district_table',
        python_callable=extract_dim_district_table,
    )

    task_4 = MySqlOperator(
        task_id="transform_district_monthly",
        mysql_conn_id="mysql_conn",
        sql="sql/district_monthly.sql",
    )

    task_5 = PythonOperator(
        task_id="load_district_monthly",
        python_callable=load_district_monthly,
    )

    task_6 = MySqlOperator(
        task_id="transform_district_yearly",
        mysql_conn_id="mysql_conn",
        sql="sql/district_yearly.sql",
    )

    task_7 = PythonOperator(
        task_id="load_district_yearly",
        python_callable=load_district_yearly,
    )

    task_8 = MySqlOperator(
        task_id="transform_province_daily",
        mysql_conn_id="mysql_conn",
        sql="sql/province_daily.sql",
    )

    task_9 = PythonOperator(
        task_id="load_province_daily",
        python_callable=load_province_daily,
    )

    task_10 = MySqlOperator(
        task_id="transform_province_monthly",
        mysql_conn_id="mysql_conn",
        sql="sql/province_monthly.sql",
    )

    task_11 = PythonOperator(
        task_id="load_province_monthly",
        python_callable=load_province_monthly,
    )

    task_12 = MySqlOperator(
        task_id="transform_province_yearly",
        mysql_conn_id="mysql_conn",
        sql="sql/province_yearly.sql",
    )

    task_13 = PythonOperator(
        task_id="load_province_yearly",
        python_callable=load_province_yearly,
    )

    end = DummyOperator(
        task_id = "end",
        dag = dag
    )

    start >> task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8 >> task_9 >> task_10 >> task_11 >> task_12 >> task_13 >> end