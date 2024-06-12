import os
import shutil
from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def prepare_file_path(**kwargs):
    ti: TaskInstance = kwargs['ti']

    execution_date = kwargs['execution_date'].strftime('%Y-%m-') + str(kwargs['execution_date'].day)

    src_file = f"/app/file_storage/raw/customers/{execution_date}/"
    dst_path = f"/app/file_storage/processed/bronze/customers/{execution_date}/"

    os.makedirs(dst_path, exist_ok=True)

    # shutil.copytree(src_file, dst_path, dirs_exist_ok=True)

    ti.xcom_push(key='src', value=src_file)
    ti.xcom_push(key='dst', value=dst_path)

def copy_file_callback(**kwargs):
    execution_date = kwargs['execution_date'].strftime('%Y-%m-') + str(kwargs['execution_date'].day)

    a = f"/app/file_storage/raw/customers/{execution_date}/"
    b = f"/app/file_storage/processed/bronze/customers/{execution_date}/"

    shutil.copytree(a, b, dirs_exist_ok=True)


with DAG(
        dag_id="customers-raw-to-bronze",
        max_active_runs=1,
        start_date=datetime.strptime('2022-08-1', '%Y-%m-%d'),
        end_date=datetime.strptime('2022-08-5', '%Y-%m-%d'),
        tags=['customers', 'bronze']
) as upload_to_bucket_dag:
    start = EmptyOperator(task_id='start', dag=upload_to_bucket_dag)

    prepare_path = PythonOperator(
        task_id='prepare_path',
        python_callable=prepare_file_path,
        provide_context=True,
    )

    copy_file = PythonOperator(
        task_id='copy_file',
        python_callable=copy_file_callback,
        provide_context=True
    )

    end = EmptyOperator(task_id='end', dag=upload_to_bucket_dag)

    start >> prepare_path >> copy_file >> end
