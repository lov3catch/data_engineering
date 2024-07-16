import json
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook


def extract_data_from_api_callback(raw_dir, date):
    request_payload = json.dumps({
        "raw_dir": raw_dir,
        "date": date,
    })

    hook = HttpHook(method='POST', http_conn_id='task2_app_job1')
    hook.run(data=request_payload, headers={'Content-Type': 'application/json'})


def convert_to_avro_callback(raw_dir, stg_dir):
    request_payload = json.dumps({
        "raw_dir": raw_dir,
        "stg_dir": stg_dir,
    })

    hook = HttpHook(method='POST', http_conn_id='task2_app_job2')
    hook.run(data=request_payload, headers={'Content-Type': 'application/json'})


with DAG(
        dag_id="process_sales",
        schedule="0 1 * * *",
        max_active_runs=1,
        catchup=True,
        start_date=datetime.strptime('2022-08-09', '%Y-%m-%d'),
        end_date=datetime.strptime('2022-08-12', '%Y-%m-%d'),
) as process_sales_dag:
    start = EmptyOperator(task_id='start', dag=process_sales_dag)

    extract_data_from_task = PythonOperator(task_id='extract_data_from_api', dag=process_sales_dag,
                                            python_callable=extract_data_from_api_callback,
                                            op_kwargs={"raw_dir": "raw/sales/{{ ds }}", "date": "{{ ds }}"})

    convert_to_avro_task = PythonOperator(task_id='convert_to_avro', dag=process_sales_dag,
                                          python_callable=convert_to_avro_callback,
                                          op_kwargs={"raw_dir": "raw/sales/{{ ds }}", "stg_dir": "stg/sales/{{ ds }}"})

    end = EmptyOperator(task_id='end', dag=process_sales_dag)

    start >> extract_data_from_task >> convert_to_avro_task >> end
