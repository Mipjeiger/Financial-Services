import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load .env from root folder (.env)
env_path = os.path.join(os.path.dirname(__file__), "../../.env")
load_dotenv(dotenv_path=env_path)

# Set default values jika environment variable tidak ada
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print(
    f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT} with user '{DB_USER}' to database '{DB_NAME}'"
)

# usage
if __name__ == "__main__":
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT current_user, current_database();")
            print(f"Connection successful: {result.fetchone()}")
    except Exception as e:
        print(f"Connection failed: {e}")
