from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="spark_fraud_etl",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingest", bash_command="spark-submit spark/bronze_ingest.py"
    )

    silver = BashOperator(
        task_id="silver_clean", bash_command="spark-submit spark/silver_cleaning.py"
    )

    gold = BashOperator(
        task_id="gold_features", bash_command="spark-submit spark/gold_features.py"
    )

    write_pg = BashOperator(
        task_id="write_postgres", bash_command="spark-submit spark/write_to_postgres.py"
    )

    bronze >> silver >> gold >> write_pg
