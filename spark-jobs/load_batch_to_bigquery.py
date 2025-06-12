from pyspark.sql import SparkSession
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--key_path", required=True)
    parser.add_argument("--run_date", required=True)
    parser.add_argument("--project_id", required=True)
    return parser.parse_args()

args = parse_args()

spark = (
    SparkSession.builder
      .appName(f"load_{args.table}_to_bigquery")
      .config("spark.jars", "/home/ali/spark/jars/spark-bigquery-with-dependencies_2.12-0.31.0.jar,/home/ali/spark/jars/hadoop-aws-3.3.1.jar,/home/ali/spark/jars/aws-java-sdk-bundle-1.12.541.jar")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
)

df = spark.read.parquet(f"s3a://finance1337/raw/{args.table}/dt={args.run_date}")

bq_table = f"{args.project_id}.stage.{args.table}"

df.write \
  .format("bigquery") \
  .option("credentialsFile", args.key_path) \
  .option("table",   bq_table) \
  .option("parentProject", args.project_id) \
  .option("writeMethod",   "direct") \
  .mode("append") \
  .save()

spark.stop()