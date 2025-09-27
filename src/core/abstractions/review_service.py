from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseReviewService(ABC):
    """Базовый класс сервиса для проведения ревью."""

    @abstractmethod
    def review(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Провести ревью входных данных.

        :param payload: Входные данные
        :return: Результат проверки
        """
        pass
