from abc import ABC, abstractmethod
from typing import Dict, List


class BaseMessageHandler(ABC):
    """Абстракция для обработки сообщений LLM."""

    @abstractmethod
    def process_messages(
        self,
        prompt: str,
        system_message: str = None,
        chat_history: List[Dict[str, str]] = None,
    ) -> str:
        """
        Обработать сообщения и получить ответ.
        :param prompt: Промпт
        :param system_message: Системное сообщение
        :param chat_history: История чата
        """
        pass
