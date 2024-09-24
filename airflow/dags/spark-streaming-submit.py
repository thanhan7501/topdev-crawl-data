from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {"owner": "An", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="spark-streaming-submit",
    default_args=default_args,
    description="start streaming jobs",
    start_date=datetime(2024, 9, 15, 0),
    schedule_interval="@once",
) as dag:
    start_bash_script = BashOperator(
        task_id="start-script", bash_command="echo start job"
    )
    
    transform_jobs_data = SparkSubmitOperator(
        task_id="transform-jobs-data",
        conn_id="spark-docker",
        application="spark/python/topdev-jobs.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"
    )

start_bash_script >> transform_jobs_data
