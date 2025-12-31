import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), "../../.env")
load_dotenv(dotenv_path=env_path)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def send_slack_alert(
    title: str, message: str, level: str = "INFO", component: str = "SYSTEM"
):
    payload = {
        "attachments": [
            {
                "color": {
                    "INFO": "#439FE0",
                    "SUCCESS": "#36a64f",
                    "WARNING": "#ffae42",
                    "ERROR": "#ff0000",
                }.get(level, "#439FE0"),
                "title": f"[{level}] {title}",
                "text": message,
                "footer": f"{component} | {datetime.utcnow()} UTC",
            }
        ]
    }

    requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
