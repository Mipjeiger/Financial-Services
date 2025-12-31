from kafka import KafkaConsumer
import json
import psycopg2
from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(dotenv_path=env_path)

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)
cursor = conn.cursor()

consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

for msg in consumer:
    event = msg.value
    cursor.execute(
        """
        INSERT INTO raw.finance (customer_id, transaction_amount, event_type, ts)
        VALUES (%s, %s, %s, to_timestamp(%s))""",
        (
            event["customer_id"],
            event["transaction_amount"],
            event["event_type"],
            event["timestamp"],
        ),
    )
    conn.commit()
