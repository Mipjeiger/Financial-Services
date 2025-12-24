import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import psycopg2

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


def load_finance_csv(file_path: str):
    df = pd.read_csv(file_path)

    # rename column
    df = df.rename(
        columns={
            "S. No.": "customer_id",
            "Total Loan": "transaction_amount",
            "LCY Deposit": "account_balance",
        }
    )

    df = df[["customer_id", "transaction_amount", "account_balance"]]

    with engine.connect() as conn:
        df.to_sql("finance", conn, schema="raw", if_exists="append", index=False)
    print(f"Data loaded into finance table.")


def load_marketing_csv(file_path: str):
    df = pd.read_csv(file_path, sep=";")

    df = df.rename(
        columns={
            "age": "customer_id",  # asumsi age sebagai proxy ID (contoh)
            "duration": "clicks",
            "campaign": "impression",
            "previous": "conversion",
        }
    )

    df = df[["customer_id", "clicks", "impression", "conversion"]]
    with engine.connect() as conn:
        df.to_sql("marketing", conn, schema="raw", if_exists="append", index=False)
    print(f"Data loaded into marketing table.")


# load data
load_finance_csv("data/raw/Finance-key.csv")
load_marketing_csv("data/raw/Marketing-key.csv")

# check if data is loaded
with engine.connect() as conn:
    finance_count = conn.execute(text("SELECT COUNT(*) FROM raw.finance")).scalar()
    marketing_count = conn.execute(text("SELECT COUNT(*) FROM raw.marketing")).scalar()

    print(f"Finance table row count: {finance_count}")
    print(f"Marketing table row count: {marketing_count}")
