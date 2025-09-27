from typing import Any, Dict

from src.core.abstractions.agent import BaseAgent
from src.core.abstractions.review_service import BaseReviewService
from src.core.logging import get_logger


class SchemaReviewService(BaseReviewService):
    """Сервис для проведения ревью схемы БД."""

    def __init__(self, agent: BaseAgent):
        self.agent = agent
        self.logger = get_logger(__name__)

    def review(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Провести ревью схемы БД на основе входных данных.
        :param payload: Входные данные для ревью
        :return: Результат ревью
        """
        self.logger.info(f"Начинается ревью схемы БД со входными данными: {payload}")

        thread_id = payload.get("thread_id")
        result = self.agent.review(
            payload=payload,
            thread_id=thread_id,
        )

        self.logger.info(
            f"Тип результата проведения ревью: {type(result)}, результат: {result}"
        )
        return result
