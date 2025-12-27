from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import logging
import pickle

# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# Load environment variables
# -------------------------------------------------
env_path = os.path.join(os.path.dirname(__file__), "../.env")
load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    raise ValueError("Missing required database environment variables")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

logger.info("Database engine initialized")

# -------------------------------------------------
# SQL Query Registry (SAFE)
# -------------------------------------------------
SQL_QUERIES = {
    "finance": "SELECT * FROM feature.finance",
    "marketing": "SELECT * FROM feature.marketing",
    "feature_marketing": "SELECT * FROM feature.marketing",
    "feature_finance": "SELECT * FROM feature.finance",
    "marketing_score": "SELECT * FROM marketing_scores",
    "feature_finance_fraud": "SELECT * FROM feature.finance_fraud_features",
    "feature_marketing_fraud": "SELECT * FROM feature.marketing_fraud_features",
    "feature_fraud": "SELECT * FROM feature.feature_fraud",
    "label_fraud": "SELECT * FROM label.fraud_label",
    "feature_fraud_dataset": "SELECT * FROM feature.training_fraud_dataset",
    "finance_fraud_daily": "SELECT * FROM feature.finance_fraud_daily",
    "marketing_fraud_daily": "SELECT * FROM feature.marketing_fraud_daily",
    "feature_fraud_daily": "SELECT * FROM feature.feature_fraud_daily",
    "fraud_label_daily": "SELECT * FROM label.fraud_label_daily",
    "training_fraud_daily": "SELECT * FROM feature.training_fraud_daily",
    "daily_fraud_alert": "SELECT * FROM alert.daily_fraud_alert",
}

# -------------------------------------------------
# Flask App
# -------------------------------------------------
app = Flask(__name__)


# -------------------------------------------------
# Data Service (OOP)
# -------------------------------------------------
class DataService:
    def __init__(self, engine):
        self.engine = engine
        self.model = None
        self.scaler = None

    def load_model(self, model_path: str):
        import tensorflow as tf

        self.model = tf.keras.models.load_model(model_path)
        logger.info(f"Model loaded: {model_path}")

    def load_scaler(self, scaler_path: str):
        with open(scaler_path, "rb") as f:
            self.scaler = pickle.load(f)
        logger.info(f"Scaler loaded: {scaler_path}")

    def predict(self, df: pd.DataFrame):
        if self.model is None or self.scaler is None:
            raise RuntimeError("Model or scaler not loaded")

        X = self.scaler.transform(df)
        return self.model.predict(X)


data_service = DataService(engine=engine)

# -------------------------------------------------
# Optional model loading
# -------------------------------------------------
try:
    data_service.load_model("notebooks/models/fraud_model.h5")
except Exception as e:
    logger.warning(f"Model not loaded: {e}")

try:
    data_service.load_scaler("notebooks/models/scaler_fraud_model.pkl")
except Exception as e:
    logger.warning(f"Scaler not loaded: {e}")


# -------------------------------------------------
# Routes
# -------------------------------------------------
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok"}), 200


@app.route("/model_check", methods=["GET"])
def model_check():
    return jsonify({"model_loaded": data_service.model is not None}), 200


@app.route("/data_check", methods=["GET"])
def data_check():
    try:
        with data_service.engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({"data_status": "connected"}), 200
    except Exception as e:
        logger.error(e)
        return jsonify({"data_status": "error", "message": str(e)}), 500


@app.route("/data_validate", methods=["POST"])
def data_validate():
    payload = request.get_json(silent=True)

    if not payload or "query_key" not in payload:
        return jsonify({"error": "query_key is required"}), 400

    query_key = payload["query_key"]
    logger.info(f"Validating dataset: {query_key}")

    if query_key not in SQL_QUERIES:
        return jsonify({"error": "Invalid query_key"}), 400

    try:
        # Fix: Use raw_connection() for SQLAlchemy 1.4.x compatibility with pandas
        raw_conn = data_service.engine.raw_connection()
        try:
            df = pd.read_sql_query(SQL_QUERIES[query_key], raw_conn)
        finally:
            raw_conn.close()

        logger.info(f"Data retrieved: {len(df)} rows, {len(df.columns)} columns")

        if df.empty:
            return (
                jsonify({"validation_status": "failed", "reason": "empty table"}),
                404,
            )

        return (
            jsonify(
                {
                    "validation_status": "success",
                    "rows": len(df),
                    "columns": list(df.columns),
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return jsonify({"validation_status": "error", "message": str(e)}), 500


# -------------------------------------------------
# Run
# -------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6789, debug=True)
