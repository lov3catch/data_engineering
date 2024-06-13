import datetime
import os
import shutil

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def prepare_file_path_callback(**kwargs):
    """
    Prepare path for sales silver data

    :param kwargs:
    :return:
    """
    execution_date = kwargs['execution_date'].strftime('%Y-%m-') + str(kwargs['execution_date'].day)
    src = os.path.join(kwargs['params']['src'], execution_date)
    dst = os.path.join(kwargs['params']['src'], execution_date)

    if os.path.isdir(src):
        os.makedirs(dst, exist_ok=True)


def copy_file_callback(**kwargs):
    """
    Copy sales data from raw to silver

    :param kwargs:
    :return:
    """
    execution_date = kwargs['execution_date'].strftime('%Y-%m-') + str(kwargs['execution_date'].day)
    src = os.path.join(kwargs['params']['src'], execution_date, f"{execution_date}__sales.csv")
    dst = os.path.join(kwargs['params']['src'], execution_date, f"{execution_date}__sales.csv")

    if os.path.isdir(src):
        shutil.copyfile(src, dst)


with DAG(
        dag_id="sales-raw-to-bronze",
        max_active_runs=1,
        start_date=datetime.datetime(year=2022, month=9, day=1),
        schedule='@daily',
        params={
            "src": Param(
                "/app/file_storage/raw/sales/",
                type="string",
                title="Raw data is here",
            ),
            "dst": Param(
                "/app/file_storage/processed/bronze/sales/",
                type="string",
                title="Processed data is here",
            ),
        },
        tags=['sales', 'bronze']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)

    prepare_path = PythonOperator(
        task_id='prepare_path',
        python_callable=prepare_file_path_callback,
        provide_context=True,
    )

    copy_file = PythonOperator(
        task_id='copy_files',
        python_callable=copy_file_callback,
        provide_context=True
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> prepare_path >> copy_file >> end
