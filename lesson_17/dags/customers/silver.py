from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_master = "spark://spark-master:7077"
spark_app_name = "DataFrameAPIExample"
file_path = "/usr/local/spark/resources/data/test.csv"

args = {
    'owner': 'spark',
}

dag = DAG(
    dag_id="customers-bronze-to-silver",
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 5),
    max_active_runs=1,
    default_args=args,
    tags=['customers', 'silver']
)

start = EmptyOperator(task_id='start', dag=dag)
run_spark_job = SparkSubmitOperator(
    dag=dag,
    task_id='run_spark_job2',
    application='/app/tasks/customers_bronze_to_silver.py',
    name=spark_app_name,
    verbose=True,
    conn_id="spark_default",
    conf={"spark.master": spark_master},
    application_args=[
    ]
)

end = EmptyOperator(task_id='end', dag=dag)

start >> run_spark_job >> end
