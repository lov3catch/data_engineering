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
        dag_id="enrich-user-profiles-silver-to-gold",
        max_active_runs=1,
        default_args=args,
        params={
            "application": Param(
                '/app/tasks/gold_enrich_user_profiles.py',
                type="string",
                title="Spark application",
            ),
        },
        tags=['user_profiles', 'gold']
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)
    run_spark_job = SparkSubmitOperator(
        dag=dag,
        task_id='enrich_users_profiles',
        application=dag.params.get('application'),
        name=spark_app_name,
        conn_id="spark_default",
        conf={"spark.master": spark_master},
    )

    end = EmptyOperator(task_id='end', dag=dag)

    start >> run_spark_job >> end
