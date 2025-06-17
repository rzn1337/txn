from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ingest_fraud_labels").getOrCreate()

df = spark.read.csv("s3a://finance1337/fraud_labels.csv", headers=True, inferSchema=True)

df.write.mode("append").parquet("s3a://finance1337/labels/")

spark.stop()