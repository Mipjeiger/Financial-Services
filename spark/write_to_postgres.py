from dotenv import load_dotenv
from pyspark.sql import SparkSession
import os

env_path = os.path.join(os.path.dirname(__file__), "../.env")
load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

spark = SparkSession.builder.appName("write-to-postgres").getOrCreate()

features = spark.read.parquet("s3a://gold/training_dataset/fraud/")

features.write.format("jdbc").option(
    "url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
).option(DB_USER).option(DB_PASSWORD).option("dbtable", "fraud_features").mode(
    "overwrite"
).save()
