import os
from dotenv import load_dotenv

load_dotenv()


def _get(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _get_int(name: str, default: int) -> int:
    val = _get(name, "")
    return int(val) if val else default


HOST = _get("HOST", "0.0.0.0")
PORT = _get_int("PORT", 8000)
API_KEY = _get("API_KEY")

MONGO_URI = _get("MONGO_URI")
DB_NAME = _get("DB_NAME", "musicbot")
COLLECTION = _get("COLLECTION", "songs")

CHANNEL_ID = _get_int("CHANNEL_ID", 0)
CHANNEL_USERNAME = _get("CHANNEL_USERNAME")

API_ID = _get_int("API_ID", 0)
API_HASH = _get("API_HASH")

SESSION_STRINGS = [
    s for s in (
        _get("SESSION_STRING_1"),
        _get("SESSION_STRING_2"),
        _get("SESSION_STRING_3"),
    ) if s
]

MAX_CONCURRENT_OPS = _get_int("MAX_CONCURRENT_OPS", 3)
MAX_FLOODWAIT_SECONDS = _get_int("MAX_FLOODWAIT_SECONDS", 60)


def validate():
    missing = []
    if not API_KEY:
        missing.append("API_KEY")
    if not MONGO_URI:
        missing.append("MONGO_URI")
    if not CHANNEL_ID:
        missing.append("CHANNEL_ID")
    if not API_ID or not API_HASH:
        missing.append("API_ID / API_HASH")
    if not SESSION_STRINGS:
        missing.append("SESSION_STRING_1 (at least one account required)")
    if missing:
        raise RuntimeError(f"Missing required .env values: {', '.join(missing)}")
