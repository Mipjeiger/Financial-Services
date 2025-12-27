FROM apache/airflow:2.9.0-python3.11

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.2

# Copy entrypoint script
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]