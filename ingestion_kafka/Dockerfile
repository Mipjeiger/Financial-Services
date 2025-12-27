FROM python:3.11-slim

WORKDIR /app

RUN pip install kafka-python psycopg2-binary

COPY ingestion_kafka/consumer_to_postgres.py .

CMD ["python", "consumer_to_postgres.py"]