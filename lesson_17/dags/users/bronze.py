import shutil

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def copy_file_callback(**kwargs):
    """
    Copy users data from raw to silver
    :param kwargs:
    :return:
    """

    shutil.copyfile(kwargs['params']['src'], kwargs['params']['dst'])


dag_params = {
    "src": Param(
        "/app/file_storage/raw/user_profiles/user_profiles.json",
        type="string",
        title="Raw data is here",
    ),
    "dst": Param(
        "/app/file_storage/processed/bronze/user_profiles/user_profiles.json",
        type="string",
        title="Processed data is here",
    ),
}

with DAG(
        dag_id="user-profiles-raw-to-bronze",
        max_active_runs=1,
        params=dag_params,
        tags=['user_profiles', 'bronze']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)

    copy_file = PythonOperator(
        task_id='copy_file',
        python_callable=copy_file_callback,
        provide_context=True
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> copy_file >> end
