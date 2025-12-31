from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    event = {
        "customer_id": random.randint(1, 10000),
        "transaction_amount": random.uniform(10, 5000),
        "event_type": random.choice(["click", "purchase", "fail_tx"]),
        "timestamp": time.time(),
    }

    producer.send("user_events", value=event)
    time.sleep(1)  # Simulate a delay between events
