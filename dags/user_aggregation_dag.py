import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="sparking_flow",
    default_args={
        "owner": "vlad_kaemzy",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    description='A simple daily DAG',
    schedule_interval='0 7 * * *',
)

spark_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/spark_job.py",
    application_args=["{{ ds }}", "{{ macros.ds_add(ds, -7) }}"],
    dag=dag,
    verbose=False
)

spark_job
