from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze-ingest").getOrCreate()

finance = spark.read.option("header", True).csv("s3a://bronze/raw_finance/finance.csv")
marketing = spark.read.option("header", True).csv(
    "s3a://bronze/raw_marketing/marketing.csv"
)

finance.write.mode("overwrite").parquet("s3a://silver/clean_finance/")
marketing.write.mode("overwrite").parquet("s3a://silver/clean_marketing/")
