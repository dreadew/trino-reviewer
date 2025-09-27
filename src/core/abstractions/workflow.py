from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseWorkflow(ABC):
    """Базовый Workflow для использования в агентах."""

    @abstractmethod
    def execute(
        self, initial_state: Dict[str, Any], thread_id: str = None
    ) -> Dict[str, Any]:
        """
        Вызвать Workflow

        :param initial_state: Начальное состояние
        :param thread_id: Идентификатор треда (для сохранения контекста)
        :return: Результат выполнения Workflow
        """
        pass
