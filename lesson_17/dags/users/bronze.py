import os
import shutil

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def copy_file_callback(**kwargs):
    a = f"/app/file_storage/raw/user_profiles/user_profiles.json"
    b = f"/app/file_storage/processed/bronze/user_profiles/user_profiles.json"

    os.makedirs(f"/app/file_storage/processed/bronze/user_profiles/", exist_ok=True)

    shutil.copyfile(a, b)


with DAG(
        dag_id="user-profiles-raw-to-bronze",
        max_active_runs=1,
        tags=['user_profiles', 'bronze']
) as upload_to_bucket_dag:
    start = EmptyOperator(task_id='start', dag=upload_to_bucket_dag)

    copy_file = PythonOperator(
        task_id='copy_file',
        python_callable=copy_file_callback,
        provide_context=True
    )

    end = EmptyOperator(task_id='end', dag=upload_to_bucket_dag)

    start >> copy_file >> end
