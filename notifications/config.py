import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_NAME = os.getenv("PROJECT_NAME", "DataCo Supply Chain")
PROJECT_VERSION = os.getenv("PROJECT_VERSION", "1.0.0")
ENVIRONMENT = os.getenv("ENVIRONMENT", "Development")
GIT_COMMIT = os.getenv("GIT_COMMIT", "unknown")

# Map .env keys or standard keys
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("telegramebot")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("chat_id")

POSTGRES_URI = os.getenv("POSTGRES_URI")

# Optional ML models mapping if needed
ML_MODEL_NAME = os.getenv("ML_MODEL_NAME", "UnknownModel")
ML_MODEL_VERSION = os.getenv("ML_MODEL_VERSION", "UnknownVersion")
