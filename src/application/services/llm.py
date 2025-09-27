from typing import List

from langchain_gigachat import GigaChat

from src.core.abstractions.llm import BaseLLMService
from src.core.logging import get_logger


class LLMService(BaseLLMService):
    def __init__(self, model: GigaChat):
        self.model = model
        self.logger = get_logger(__name__)

    def invoke_with_messages(self, messages: List) -> str:
        try:
            response = self.model.invoke(messages)
            return response.content
        except Exception as e:
            self.logger.error(f"Неизвестная ошибка в сервисе LLM: {e}")
            raise

    def invoke_with_prompt(self, prompt: str, system_message: str = None) -> str:
        from langchain.schema import HumanMessage, SystemMessage

        messages = []
        if system_message:
            messages.append(SystemMessage(content=system_message))
        messages.append(HumanMessage(content=prompt))

        return self.invoke_with_messages(messages)
