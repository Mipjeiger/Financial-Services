from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver-cleaning").getOrCreate()
finance = spark.read.parquet("s3a://silver/clean_finance/")

finance_clean = (
    finance.withColumn("transaction_amount", col("transaction_amount").cast("double"))
    .withColumn("failed_tx", when(col("event_type") == "fail_tx", 1).otherwise(0))
    .dropna()
)

finance_clean.write.mode("overwrite").parquet("s3a://gold/fraud_features/finance/")
