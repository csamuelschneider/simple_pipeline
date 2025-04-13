from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os

dag_directory = os.path.dirname(os.path.abspath(__file__))

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

    begin = EmptyOperator(task_id='begin', dag=dag)
    end = EmptyOperator(task_id='end', dag=dag)

begin >> load_postgres_raw_sales >> load_postgres_trusted_sales >> end