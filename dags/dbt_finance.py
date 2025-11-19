

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
import os 

default_args = {
    'owner': 'tam.dang',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

finance_dag = DAG(
    'dbt_finance',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'finance', 'transformation'],
)

if os.path.exists('/opt/airflow'):
    project_dir = '/opt/airflow'
    credentials_path = '/opt/airflow/config/service_account.json'
else:
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    credentials_path = os.path.join(project_dir, 'config', 'service_account.json')

project_id = 'cusma-383203'
dataset_id = 'dwh_prod'

wait_for_ingestion = ExternalTaskSensor(
    task_id='wait_for_data_ingestion',
    external_dag_id='data_ingestion',
    external_task_id='load_raw_data_to_bigquery',
    timeout=3600,
    poke_interval=300,
    mode='reschedule',
    dag=finance_dag,
)

dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f'cd {project_dir} && dbt deps',
    dag=finance_dag,
)

dbt_run_finance_staging = BashOperator(
    task_id='dbt_run_finance_staging',
    bash_command=f'cd {project_dir} && dbt run --select finance.staging --target prod',
    dag=finance_dag,
)

dbt_test_finance_staging = BashOperator(
    task_id='dbt_test_finance_staging',
    bash_command=f'cd {project_dir} && dbt test --select finance.staging --target prod',
    dag=finance_dag,
)

dbt_run_finance_intermediate = BashOperator(
    task_id='dbt_run_finance_intermediate',
    bash_command=f'cd {project_dir} && dbt run --select finance.intermediate --target prod',
    dag=finance_dag,
)
dbt_run_finance_marts = BashOperator(
    task_id='dbt_run_finance_marts',
    bash_command=f'cd {project_dir} && dbt run --select finance.marts --target prod',
    dag=finance_dag,
)

dbt_test_finance_all = BashOperator(
    task_id='dbt_test_finance_all',
    bash_command=f'cd {project_dir} && dbt test --select finance --target prod',
    dag=finance_dag,
)

dbt_docs_generate = BashOperator(
    task_id='dbt_docs_generate',
    bash_command=f'cd {project_dir} && dbt docs generate --target prod',
    dag=finance_dag,
)

init = EmptyOperator(
    task_id='init_finance',
    dag=finance_dag,
)

end = EmptyOperator(
    task_id='end_finance',
    dag=finance_dag,
)


wait_for_ingestion >> dbt_deps >> dbt_run_finance_staging >> dbt_test_finance_staging >> dbt_run_finance_intermediate >> dbt_run_finance_marts >> dbt_test_finance_all >> dbt_docs_generate

