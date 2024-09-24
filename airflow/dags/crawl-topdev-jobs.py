from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {"owner": "An", "retries": 5, "retry_delay": timedelta(minutes=2)}

logger = logging.getLogger(__name__)
logger.info("This is a log message")


def check_polling(response):
    res = response.json()
    if res["currstate"] == "finished":
        return True

    return False


with DAG(
    dag_id="crawl-topdev-jobs",
    default_args=default_args,
    description="Crawl jobs data from topdev",
    start_date=datetime(2024, 9, 15, 0),
    schedule_interval="@daily",
) as dag:
    start_bash_script = BashOperator(
        task_id="start-script", bash_command="echo start job"
    )

    crawl_jobs = HttpOperator(
        task_id="crawl-jobs",
        http_conn_id="scrapyd",
        method="post",
        endpoint="schedule.json",
        data={"project": "topdev", "spider": "jobs"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        log_response=True,
        do_xcom_push=True,
        response_filter=lambda response: response.json()["jobid"],
    )

start_bash_script >> crawl_jobs
