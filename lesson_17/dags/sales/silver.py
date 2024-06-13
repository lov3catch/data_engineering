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

with DAG(
        dag_id="sales-bronze-to-silver",
        start_date=datetime.datetime(year=2022, month=9, day=1),
        schedule='@daily',
        max_active_runs=1,
        default_args=args,
        params={
            "application": Param(
                '/app/tasks/sales_bronze_to_silver.py',
                type="string",
                title="Spark application",
            ),
        },
        tags=['sales', 'silver']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)

    run_spark_job = SparkSubmitOperator(
        dag=dag,
        task_id='normalize_sales_data',
        application=dag.params.get('application'),
        name=spark_app_name,
        conn_id="spark_default",
        conf={"spark.master": spark_master},
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> run_spark_job >> end
