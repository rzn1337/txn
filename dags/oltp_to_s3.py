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

RUN_PHASE1 = (
    Variable.get("RUN_PHASE1", default_var="true")
    .lower() in ("1", "true", "yes")
)

with DAG (
    dag_id="oltp_to_s3",
    default_args=default_args,
    start_date=datetime(2025, 6, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    tables = ["transactions", "customer_accounts", "customer_profiles", "ip_intelligence", "merchant_profiles"]

    @task
    def get_watermark(table: str) -> str:
        return Variable.get(f"wm__{table}")

    @task
    def set_watermark_from_s3(table: str):
        aws_access_key = Variable.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = Variable.get("AWS_SECRET_ACCESS_KEY")
        s3 = boto3.client("s3",aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
        bucket = "finance1337"
        key = f"watermarks/{table}.txt"
        obj = s3.get_object(Bucket=bucket, Key=key)
        new_wm = obj["Body"].read().decode("utf-8").strip()
        Variable.set(f"wm__{table}", new_wm)


    oltp_to_s3_done = EmptyOperator(task_id="oltp_to_s3_done")

    for table in tables:

        if RUN_PHASE1:

            wm = get_watermark.override(task_id=f"get_wm__{table}")(table)

            run_spark_job = SparkSubmitOperator(
                task_id=f"spark_ingest__{table}",
                application="/home/ali/airflow/spark-jobs/write_batch_to_s3.py",
                conn_id="spark_conn",
                jars="/home/ali/spark/jars/postgresql-42.6.0.jar,/home/ali/spark/jars/hadoop-aws-3.3.1.jar,/home/ali/spark/jars/aws-java-sdk-bundle-1.12.541.jar",
                conf={
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                },
                executor_cores=2,
                executor_memory="4g",
                num_executors=2,
                env_vars={
                    "PG_USER": "{{ var.value.PG_USER }}",
                    "PG_PASSWORD": "{{ var.value.PG_PASSWORD }}",
                    "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
                    "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
                },
                application_args=[
                        "--table", table,
                        "--since", f"{{{{ ti.xcom_pull(task_ids='get_wm__{table}') }}}}",
                        "--run_date", "{{ ds }}"
                ],
                # do_xcom_push=True,
                # params={
                #     "table": table,
                #     "run_date", "{{ ds }}"
                # },
            )

            upd = set_watermark_from_s3.override(task_id=f"set_wm__{table}")(table)

            wm >> run_spark_job >> upd >> oltp_to_s3_done

                
    for table in tables:
        run_spark_job_2 = SparkSubmitOperator(
            task_id=f"spark_stream_{table}_to_bigquery",
            application="/home/ali/airflow/spark-jobs/load_batch_to_bigquery.py",
            conn_id="spark_conn",
            jars="/home/ali/spark/jars/spark-bigquery-with-dependencies_2.12-0.31.0.jar,/home/ali/spark/jars/hadoop-aws-3.3.1.jar,/home/ali/spark/jars/aws-java-sdk-bundle-1.12.541.jar",
            conf={
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            },
            executor_cores=1,
            executor_memory="2g",
            num_executors=1,
            env_vars={
                "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
                "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
            },
            application_args=[
                    "--table", table,
                    "--key_path", "{{var.value.GCP_KEY_PATH}}",
                    "--project_id", "{{ var.value.GCP_PROJECT_ID }}",
                    # "--run_date", "{{ ds }}"
                    "--run_date", "2025-06-07"
            ],    
            pool="bq_stream_pool",        
        )

        if RUN_PHASE1:
            oltp_to_s3_done >> run_spark_job_2
        else:
            run_spark_job_2