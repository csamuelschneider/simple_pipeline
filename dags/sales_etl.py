from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from psycopg2 import sql
from duplicate_key import DataQuality as dq
import os

def dq_duplicates(table_name, key_columns):
    connection_params = {
        'host': os.getenv('POSTGRES_HOST'),
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
        'port': os.getenv('POSTGRES_PORT')
    }
    
    dq_instance = dq(connection_params)

    if dq_instance.check_duplicate_keys(table_name, key_columns):
        raise print(f"/-----------------Chaves duplicadas na tabela {table_name}-----------------/")
    else:
        print(f"Não foram encontrados duplicados na tabela {table_name}")

with DAG(
    dag_id='dag_sales',
    start_date=datetime(2023, 10, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:
    
    load_postgres_raw_sales = BashOperator(
        task_id='load_postgres_raw_sales',
        bash_command='python /opt/airflow/scripts/raw_sales.py',
        dag=dag
    )

    load_postgres_trusted_sales = BashOperator(
        task_id='load_postgres_trusted_sales',
        bash_command=f'python /opt/airflow/scripts/trusted_sales.py',        
        dag=dag
    )

    duplicates_dq = PythonOperator(
        task_id='duplicates_dq',
        python_callable=dq_duplicates,
        op_kwargs={
            'table_name': 'trusted_sales',
            'key_columns': ['id']
        },
    )

    begin = EmptyOperator(task_id='begin', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)

begin >> load_postgres_raw_sales >> load_postgres_trusted_sales >> duplicates_dq >> end