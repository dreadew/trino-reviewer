from abc import ABC, abstractmethod
from typing import List

from src.core.abstractions.chat_message import BaseChatMessage


class ChatModel(ABC):
    """Абстракция для модели чата."""

    @abstractmethod
    def invoke(self, messages: List[BaseChatMessage]) -> str:
        """
        Вызвать модель с набором сообщений.

        :param messages: Список сообщений
        :return: Ответ модели
        """
        pass

    @abstractmethod
    def get_model_name(self) -> str:
        """
        Получить название модели.

        :return: Название модели
        """
        pass
