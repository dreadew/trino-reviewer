from langfuse import Langfuse
from langfuse.langchain import CallbackHandler

from src.core.config import config

langfuse = Langfuse(
    secret_key=config.LANGFUSE_SECRET_KEY,
    public_key=config.LANGFUSE_PUBLIC_KEY,
    host=config.LANGFUSE_HOST,
)

callback_handler = CallbackHandler()
