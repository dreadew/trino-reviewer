from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from src.core.abstractions.chat_model import ChatModel


class BaseAgent(ABC):
    """Базовый класс для агентов."""

    def __init__(self, model: Optional[ChatModel] = None):
        """
        Инициализация агента.

        :param model: Модель чата (опциональная)
        """
        self.model = model

    @abstractmethod
    def review(
        self,
        payload: Dict[str, Any],
        thread_id: str = None,
    ) -> Dict[str, Any]:
        """
        Проверить входные данные и выполнить анализ.

        :param payload: Входные данные для анализа
        :param thread_id: Идентификатор треда (для сохранения контекста)
        :return: Результат анализа
        """
        pass
