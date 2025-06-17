from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
import boto3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG (
    dag_id="ml",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    ingest = SparkSubmitOperator(
        task_id="spark_ingest_labels",
        application="/home/ali/airflow/spark-jobs/ingest_labels.py",
        conn_id="spark_conn",
        jars="/home/ali/spark/jars/postgresql-42.6.0.jar,/home/ali/spark/jars/hadoop-aws-3.3.1.jar,/home/ali/spark/jars/aws-java-sdk-bundle-1.12.541.jar",
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        executor_cores=2,
        executor_memory="4g",
        num_executors=2,
    )