
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os

default_args = {
    'owner': 'tam.dang',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

ingestion_dag = DAG(
    'data_ingestion',
    default_args=default_args,
    description='Data Ingestion Pipeline: Fetch rates from API and load raw data to BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['ingestion', 'api', 'binance', 'bigquery'],
)

if os.path.exists('/opt/airflow'):
    project_dir = '/opt/airflow'
    credentials_path = '/opt/airflow/config/service_account.json'
else:
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    credentials_path = os.path.join(project_dir, 'config', 'service_account.json')

project_id = 'cusma-383203'
dataset_id = 'raw_data'

# Fetch exchange rates from Binance API
fetch_rates = BashOperator(
    task_id='fetch_rates_from_binance',
    bash_command=f'cd {project_dir} && PYTHONPATH={project_dir}:$PYTHONPATH python scripts/fetch_kline.py',
    dag=ingestion_dag,
)

#  Load raw data to BigQuery
load_raw_data = BashOperator(
    task_id='load_raw_data_to_bigquery',
    bash_command=f'cd {project_dir} && PYTHONPATH={project_dir}:$PYTHONPATH python scripts/load_to_bigquery.py',
    dag=ingestion_dag,
)
init = EmptyOperator(
    task_id='init',
    dag=ingestion_dag,
)

end = EmptyOperator(
    task_id='end',
    dag=ingestion_dag,
)



# Define task dependencies
init >> fetch_rates >> load_raw_data >> end

