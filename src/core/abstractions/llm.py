from abc import ABC, abstractmethod
from typing import List


class BaseLLMService(ABC):
    """Сервис для операций с LLM."""

    @abstractmethod
    def invoke_with_messages(self, messages: List) -> str:
        """
        Вызвать LLM со списком сообщений.

        :param messages: Список сообщений
        """
        pass

    @abstractmethod
    def invoke_with_prompt(self, prompt: str, system_message: str = None) -> str:
        """
        Вызвать LLM с промптом.

        :param prompt: Промпт
        :param system_message: Системное сообщение
        """
        pass
