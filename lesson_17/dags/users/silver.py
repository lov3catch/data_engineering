from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_master = "spark://spark-master:7077"
spark_app_name = "DataFrameAPIExample"

args = {
    'owner': 'spark',
}

dag = DAG(
    dag_id="user-profiles-bronze-to-silver",
    max_active_runs=1,
    default_args=args,
    tags=['user_profiles', 'silver']
)

start = EmptyOperator(task_id='start', dag=dag)
run_spark_job = SparkSubmitOperator(
    dag=dag,
    task_id='run_spark_job3',
    application='/app/tasks/user_profiles_bronze_to_silver.py',
    name=spark_app_name,
    verbose=True,
    conn_id="spark_default",
    conf={"spark.master": spark_master},
    application_args=[
    ]
)

end = EmptyOperator(task_id='end', dag=dag)

start >> run_spark_job >> end
