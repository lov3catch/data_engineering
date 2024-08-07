import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_master = "spark://spark-master:7077"
spark_app_name = "DataFrameAPIExample"

args = {
    'owner': 'spark',
}

dag_params = {
    "application": Param(
        '/app/tasks/customers_bronze_to_silver.py',
        type="string",
        title="Spark application",
    ),
}

with DAG(
        dag_id="customers-bronze-to-silver",
        start_date=datetime.datetime(year=2022, month=8, day=1),
        schedule='@daily',
        max_active_runs=1,
        default_args=args,
        params=dag_params,
        tags=['customers', 'silver']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)
    run_spark_job = SparkSubmitOperator(
        dag=dag,
        task_id='normalize_customers_data',
        application=dag.params.get('application'),
        name=spark_app_name,
        conn_id="spark_default",
        conf={"spark.master": spark_master},
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> run_spark_job >> end
