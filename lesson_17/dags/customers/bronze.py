import datetime
import os
import shutil

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def prepare_file_path_callback(**kwargs):
    """
    Prepare path for customers silver data
    :param kwargs:
    :return:
    """
    execution_date = kwargs['execution_date'].strftime('%Y-%m-') + str(kwargs['execution_date'].day)
    src = os.path.join(kwargs['params']['src'], execution_date)
    dst = os.path.join(kwargs['params']['dst'], execution_date)

    if os.path.isdir(src):
        os.makedirs(dst, exist_ok=True)


def copy_file_callback(**kwargs):
    """
    Copy customers data from raw to silver
    :param kwargs:
    :return:
    """
    execution_date = kwargs['execution_date'].strftime('%Y-%m-') + str(kwargs['execution_date'].day)
    src = os.path.join(kwargs['params']['src'], execution_date)
    dst = os.path.join(kwargs['params']['dst'], execution_date)

    if os.path.isdir(src):
        shutil.copytree(src, dst, dirs_exist_ok=True)


with DAG(
        dag_id="customers-raw-to-bronze",
        max_active_runs=1,
        start_date=datetime.datetime(year=2022, month=8, day=1),
        schedule='@daily',
        params={
            "src": Param(
                "/app/file_storage/raw/customers/",
                type="string",
                title="Raw data is here",
            ),
            "dst": Param(
                "/app/file_storage/processed/bronze/customers/",
                type="string",
                title="Processed data is here",
            ),
        },
        tags=['customers', 'bronze']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)

    prepare_path = PythonOperator(
        task_id='prepare_path',
        python_callable=prepare_file_path_callback,
        provide_context=True,
    )

    copy_file = PythonOperator(
        task_id='copy_file',
        python_callable=copy_file_callback,
        provide_context=True
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> prepare_path >> copy_file >> end
