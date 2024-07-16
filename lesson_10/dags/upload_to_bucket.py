from datetime import datetime

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


def prepare_file_path(**kwargs):
    ti: TaskInstance = kwargs['ti']

    execution_date = kwargs['execution_date']
    src_file = f"/home/airflow/sales/{execution_date.strftime('%Y-%m-%d')}/sales-{execution_date.strftime('%Y-%m-%d')}.json"
    dst_path = f"src1/sales/v1/year={execution_date.year}/month={execution_date.strftime('%m')}/day={execution_date.strftime('%d')}/"

    ti.xcom_push(key='src', value=src_file)
    ti.xcom_push(key='dst', value=dst_path)


with DAG(
        dag_id="upload_to_bucket",
        max_active_runs=1,
        start_date=datetime.strptime('2022-08-09', '%Y-%m-%d'),
        end_date=datetime.strptime('2022-08-10', '%Y-%m-%d'),
) as upload_to_bucket_dag:
    start = EmptyOperator(task_id='start', dag=upload_to_bucket_dag)
    
    prepare_path = PythonOperator(
        task_id='prepare_path',
        python_callable=prepare_file_path,
        provide_context=True,
    )

    upload = LocalFilesystemToGCSOperator(
        task_id='upload',
        src="{{ ti.xcom_pull(key='src', task_ids='prepare_path') }}",
        dst="{{ ti.xcom_pull(key='dst', task_ids='prepare_path') }}",
        bucket='de-rd-course-bucket',
        gcp_conn_id="google_cloud_de",
    )

    end = EmptyOperator(task_id='end', dag=upload_to_bucket_dag)

    start >> prepare_path >> upload >> end
