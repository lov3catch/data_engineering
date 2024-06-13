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
        dag_id="leaderboard",
        max_active_runs=1,
        default_args=args,
        params={
            "application": Param(
                '/app/tasks/gold_sales_leaderboard.py',
                type="string",
                title="Spark application",
            ),
            "min_age": Param(
                20,
                type="integer",
                title="Customer min age",
            ),
            "max_age": Param(
                30,
                type="integer",
                title="Customer max age",
            ),
            "product": Param(
                'tv',
                type="string",
                title="Product type: tv, etc",
            ),
            "purchase_period_from": Param(
                default=datetime.date(year=2022, month=9, day=1).isoformat(),
                type="string",
                format="date",
                title="Purchase date from",
            ),
            "purchase_period_to": Param(
                datetime.date(year=2022, month=9, day=10).isoformat(),
                type="string",
                format="date",
                title="Purchase date to",
            ),
        },
        tags=['leaderboard', 'gold']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)
    run_spark_job = SparkSubmitOperator(
        dag=dag,
        task_id='calc_sales_leaderboard',
        application=dag.params.get('application'),
        application_args=[
            "{{ params.min_age }}",
            "{{ params.max_age }}",
            "{{ params.product }}",
            "{{ params.purchase_period_from }}",
            "{{ params.purchase_period_to }}"
        ],
        name=spark_app_name,
        conn_id="spark_default",
        conf={"spark.master": spark_master},
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> run_spark_job >> end
