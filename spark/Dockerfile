FROM bitnami/spark:3.5

USER root

RUN pip install --no-cache-dir \
    python-dotenv==1.0.1 \
    psycopg2-binary==2.9.11

# Declare ENV keys only (no values)
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_REGION=""

WORKDIR /app

USER 1001
