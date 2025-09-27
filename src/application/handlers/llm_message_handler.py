from typing import Dict, List

from langchain.schema import AIMessage, HumanMessage, SystemMessage

from src.core.abstractions.llm import BaseLLMService
from src.core.abstractions.message_handler import BaseMessageHandler
from src.core.logging import get_logger


class LLMMessageHandler(BaseMessageHandler):
    """Обработчик сообщений для LLM."""

    def __init__(self, llm_service: BaseLLMService):
        self.llm_service = llm_service
        self.logger = get_logger(__name__)

    def process_messages(
        self,
        prompt: str,
        system_message: str = None,
        chat_history: List[Dict[str, str]] = None,
    ) -> str:
        messages = []

        if system_message:
            messages.append(SystemMessage(content=system_message))

        if chat_history:
            for msg in chat_history:
                if msg["role"] == "user":
                    messages.append(HumanMessage(content=msg["content"]))
                elif msg["role"] == "assistant":
                    messages.append(AIMessage(content=msg["content"]))

        messages.append(HumanMessage(content=prompt))

        try:
            response = self.llm_service.invoke_with_messages(messages)
            self.logger.info(f"Успешно получен ответ от LLM: {response}")
            return response
        except Exception as e:
            self.logger.error(f"Ошибка при обработке сообщений LLM: {e}")
            raise
