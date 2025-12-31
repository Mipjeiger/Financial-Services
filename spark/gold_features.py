from pyspark.sql.functions import avg, sum, count
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold-features").getOrCreate()

finance = spark.read.parquet("s3a://gold/fraud_features/finance/")

features = finance.groupBy("customer_id").agg(
    avg("transaction_amount").alias("avg_tx_amount"),
    sum("failed_tx").alias("failed_tx_count"),
    count("*").alias("tx_count"),
)
features.write.mode("overwrite").parquet("s3a://gold/training_dataset/fraud/")
