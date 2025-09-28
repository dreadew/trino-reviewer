import os

from dotenv import load_dotenv

load_dotenv()
if os.path.exists(".env.local"):
    load_dotenv(".env.local", override=True)


class Config:
    """
    Конфигурация приложения
    """

    APP_NAME = os.getenv("APP_NAME", "SQL RecSys")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    MODEL_TYPE = os.getenv("MODEL_TYPE", "giga").lower()
    MODEL_NAME = os.getenv("MODEL_NAME", "GigaChat")
    API_KEY = os.getenv("API_KEY")
    MAX_TOKENS = int(os.getenv("MAX_TOKENS", "2048"))

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("API_KEY")
    OPENAI_MODEL_NAME = os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini")
    OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL")

    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("API_KEY")
    GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-1.5-flash")

    TEMPERATURE = float(os.getenv("TEMPERATURE", "0.1"))

    GRPC_PORT = int(os.getenv("GRPC_PORT", "50051"))
    GRPC_MAX_WORKERS = int(os.getenv("GRPC_MAX_WORKERS", "10"))
    GRPC_GRACE_PERIOD = int(os.getenv("GRPC_GRACE_PERIOD", "5"))

    MAX_CHAT_HISTORY_SIZE = int(os.getenv("MAX_CHAT_HISTORY_SIZE", "10"))

    DEFAULT_THREAD_ID = os.getenv("DEFAULT_THREAD_ID", "default")

    VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
    VALKEY_PORT = int(os.getenv("VALKEY_PORT", "6379"))
    VALKEY_DB = int(os.getenv("VALKEY_DB", "0"))
    VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD")

    PROMPT_CACHE_TTL = int(os.getenv("PROMPT_CACHE_TTL", "3600"))


config = Config()
