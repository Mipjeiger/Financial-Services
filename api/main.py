from flask import Request, jsonify, Flask
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

env_path = os.path.join(os.path.dirname(__file__), "../../.env")
load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

app = Flask(__name__)


class DataService:
    def __init__(self, model, engine):
        self.engine = engine
        self.model = model

    def load_model(self, model_path: str):
        import tensorflow as tf

        self.model = tf.keras.models.load_model(model_path)
        logger.info(f"Model loaded from {model_path}")

    def predict(self, input_data: pd.DataFrame):
        predictions = self.model.predict(input_data)
        return predictions


# Initialize DataService with model path
data_service = DataService(model=None, engine=engine)
data_service.load_model("models/fraud_model.h5")
data_service.scaler = pd.read_pickle("models/scaler_fraud_model.pkl")
