import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.environ.get("DB_NAME", "MoviesBot")

ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
STORAGE_CHANNEL = int(os.environ.get("STORAGE_CHANNEL", "0"))
BACKUP_CHANNEL = int(os.environ.get("BACKUP_CHANNEL", "0"))
