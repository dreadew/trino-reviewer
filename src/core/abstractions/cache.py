from abc import ABC, abstractmethod
from typing import Any, Optional


class BaseCache(ABC):
    """Базовая абстракция для сервиса кэширования."""

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """
        Получить значение по ключу.

        :param key: Ключ
        :return: Значение или None
        """
        pass

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Установить значение по ключу.

        :param key: Ключ
        :param value: Значение
        :param ttl: Время жизни в секундах (None = без ограничения)
        :return: True если успешно
        """
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Удалить значение по ключу.

        :param key: Ключ
        :return: True если успешно
        """
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Проверить существование ключа.

        :param key: Ключ
        :return: True если существует
        """
        pass

    @abstractmethod
    async def get_keys_by_pattern(self, pattern: str) -> list[str]:
        """
        Получить список ключей по шаблону.

        :param pattern: Шаблон поиска (например, "prefix:*")
        :return: Список найденных ключей
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Закрыть соединение."""
        pass
