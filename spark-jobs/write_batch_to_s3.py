from pyspark.sql import SparkSession
import os
from datetime import datetime, timedelta
import argparse
from pyspark.sql.functions import col, max as spark_max
import boto3

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--run_date", required=True)
    return parser.parse_args()

args = parse_args()

UPDATED_COL_MAP = {
    "transactions": "ingestion_timestamp",
    "customer_accounts": "last_account_update",
    "customer_profiles": "last_profile_update",
    "merchant_profiles": "last_merchant_update",
    "ip_intelligence": "last_updated"
}

args.since = args.since.replace("+00", "+00:00") # match with pg iso8601

last_updated = UPDATED_COL_MAP.get(args.table)

spark = (
    SparkSession.builder
    .appName(f"spark_ingest_{args.table}")
    .config("spark.jars", "/home/ali/spark/jars/postgresql-42.6.0.jar,/home/ali/spark/jars/hadoop-aws-3.3.1.jar,/home/ali/spark/jars/aws-java-sdk-bundle-1.12.541.jar")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# batch_size = 2000

jdbc_url = "jdbc:postgresql://localhost:5431/postgres"
conn_properties = {
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"(SELECT * FROM {args.table} WHERE {last_updated} >= '{args.since}') tmp") \
    .options(**conn_properties) \
    .load()

df.write.mode("append").parquet(f"s3a://finance1337/raw/{args.table}/dt={args.run_date}")

max_ts = df.agg(spark_max(col(last_updated))).collect()[0][0]

new_wm = max_ts.isoformat() if max_ts is not None else args.since

# print(f"[AIRFLOW_XCOM_OUTPUT] {{\"new_wm\": \"{new_wm}\"}}")

s3 = boto3.client('s3')
bucket = 'finance1337'
key = f"watermarks/{args.table}.txt"

s3.put_object(Bucket=bucket, Key=key, Body=new_wm)

spark.stop()
