from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
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
    dag_id="crawl-topdev-companies",
    default_args=default_args,
    description="Crawl companies data from topdev",
    start_date=datetime(2024, 9, 15, 0),
    schedule_interval="@once",
) as dag:
    start_bash_script = BashOperator(
        task_id="start-script", bash_command="echo start job"
    )

    crawl_companies = HttpOperator(
        task_id="crawl-companies",
        http_conn_id="scrapyd",
        method="post",
        endpoint="schedule.json",
        data={"project": "topdev", "spider": "companies"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        log_response=True,
        do_xcom_push=True,
        response_filter=lambda response: response.json()["jobid"],
    )

    wait_for_crawler = HttpSensor(
        task_id="wait-for-crawler",
        http_conn_id="scrapyd",
        method="get",
        endpoint="status.json?job={{ ti.xcom_pull(task_ids='crawl-companies') }}",
        mode="poke",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        response_check=check_polling,
    )

    transform_companies_data = SparkSubmitOperator(
        task_id="transform-companies-data",
        conn_id="spark-docker",
        application="spark/python/topdev-companies.py",
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.postgresql:postgresql:42.7.3",
    )

    # transform_companies_data = SparkSubmitOperator(
    #     task_id="transform-companies-data",
    #     conn_id="spark-docker",
    #     java_class="loadCompaniesDataToDB",
    #     application="spark/scala/target/scala-2.12/project4-topdev-spark_2.12-0.1.0-SNAPSHOT.jar",
    #     packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.postgresql:postgresql:42.7.3"
    # )

start_bash_script >> crawl_companies >> wait_for_crawler >> transform_companies_data
