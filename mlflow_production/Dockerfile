FROM python:3.11-slim

WORKDIR /app

COPY mlflow_production/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mlflow_production/ .

CMD ["python", "train.py"]
