import asyncio
import json
from typing import Any, Optional

import valkey

from src.core.abstractions.cache import BaseCache
from src.core.logging import get_logger

logger = get_logger(__name__)


class ValkeyCache(BaseCache):
    """Реализация кэша с использованием Valkey."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
    ):
        """
        Инициализация Valkey клиента.

        :param host: Хост Valkey
        :param port: Порт Valkey
        :param db: Номер базы данных
        :param password: Пароль (если требуется)
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self._client = None

    def _get_client(self) -> valkey.Valkey:
        """Получить клиент Valkey с ленивой инициализацией."""
        if self._client is None:
            self._client = valkey.Valkey(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=True,
            )
        return self._client

    async def get(self, key: str) -> Optional[Any]:
        """
        Получить значение по ключу.

        :param key: Ключ
        :return: Значение или None
        """
        try:
            client = self._get_client()

            value = await asyncio.get_event_loop().run_in_executor(
                None, client.get, key
            )

            if value is None:
                return None

            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value

        except Exception as e:
            logger.error(f"Ошибка получения из кэша [{key}]: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Установить значение по ключу.

        :param key: Ключ
        :param value: Значение
        :param ttl: Время жизни в секундах
        :return: True если успешно
        """
        try:
            client = self._get_client()

            if isinstance(value, str):
                serialized_value = value
            else:
                serialized_value = json.dumps(value, ensure_ascii=False)

            if ttl:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: client.setex(key, ttl, serialized_value)
                )
            else:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, client.set, key, serialized_value
                )

            logger.debug(f"Сохранено в кэш [{key}] TTL={ttl}")
            return bool(result)

        except Exception as e:
            logger.error(f"Ошибка сохранения в кэш [{key}]: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Удалить значение по ключу.

        :param key: Ключ
        :return: True если успешно
        """
        try:
            client = self._get_client()

            result = await asyncio.get_event_loop().run_in_executor(
                None, client.delete, key
            )

            logger.debug(f"Удален из кэша [{key}]")
            return bool(result)

        except Exception as e:
            logger.error(f"Ошибка удаления из кэша [{key}]: {e}")
            return False

    def get_sync(self, key: str) -> Optional[Any]:
        """
        Синхронно получить значение по ключу.

        :param key: Ключ
        :return: Значение или None
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.get(key))
            loop.close()
            return result
        except Exception as e:
            logger.error(f"Ошибка синхронного получения из кэша [{key}]: {e}")
            return None

    def set_sync(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Синхронно установить значение по ключу.

        :param key: Ключ
        :param value: Значение
        :param ttl: Время жизни в секундах
        :return: True если успешно
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.set(key, value, ttl))
            loop.close()
            return result
        except Exception as e:
            logger.error(f"Ошибка синхронной установки в кэш [{key}]: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """
        Проверить существование ключа.

        :param key: Ключ
        :return: True если существует
        """
        try:
            client = self._get_client()

            result = await asyncio.get_event_loop().run_in_executor(
                None, client.exists, key
            )

            return bool(result)

        except Exception as e:
            logger.error(f"Ошибка проверки существования ключа [{key}]: {e}")
            return False

    async def get_keys_by_pattern(self, pattern: str) -> list[str]:
        """
        Получить список ключей по шаблону.

        :param pattern: Шаблон поиска (например, "prefix:*")
        :return: Список найденных ключей
        """
        try:
            client = self._get_client()

            keys = await asyncio.get_event_loop().run_in_executor(
                None, client.keys, pattern
            )

            logger.debug(f"Найдено {len(keys)} ключей по шаблону [{pattern}]")
            return keys if keys else []

        except Exception as e:
            logger.error(f"Ошибка поиска ключей по шаблону [{pattern}]: {e}")
            return []

    async def close(self) -> None:
        """Закрыть соединение."""
        if self._client:
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._client.close)
                logger.info("Соединение с Valkey закрыто")
            except Exception as e:
                logger.error(f"Ошибка закрытия соединения с Valkey: {e}")
            finally:
                self._client = None


def create_valkey_cache(
    host: str = "localhost",
    port: int = 6379,
    db: int = 0,
    password: Optional[str] = None,
) -> ValkeyCache:
    """
    Фабричная функция для создания Valkey кэша.

    :param host: Хост Valkey
    :param port: Порт Valkey
    :param db: Номер базы данных
    :param password: Пароль
    :return: Настроенный ValkeyCache
    """
    return ValkeyCache(host=host, port=port, db=db, password=password)
