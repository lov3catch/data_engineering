import json
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator

process_sales_dag = DAG(
    dag_id="process_sales",
    schedule="0 1 * * *",
    max_active_runs=1,
    catchup=True,
    start_date=datetime.strptime('2022-08-09', '%Y-%m-%d'),
    end_date=datetime.strptime('2022-08-11', '%Y-%m-%d'),
)

extract_data_from_api_params = {
    "task_id": 'extract_data_from_api',
    "http_conn_id": 'task2_app_job1',
    "dag": process_sales_dag,
    "method": 'POST',
    "data": json.dumps({
        "date": "2022-08-09",
        "raw_dir": "raw/sales/2022-08-09"
    }),
    "headers": {'Content-Type': 'application/json'},
}

convert_to_avro_params = {
    "task_id": 'convert_to_avro',
    "http_conn_id": 'task2_app_job2',
    "dag": process_sales_dag,
    "method": 'POST',
    "data": json.dumps({
        "raw_dir": "raw/sales/2022-08-09",
        "stg_dir": "stg/sales/2022-08-09"
    }),
    "headers": {'Content-Type': 'application/json'},
}

start = EmptyOperator(
    task_id='start',
    dag=process_sales_dag,
)

end = EmptyOperator(
    task_id='end',
    dag=process_sales_dag,
)

start >> HttpOperator(**extract_data_from_api_params) >> HttpOperator(**convert_to_avro_params) >> end
