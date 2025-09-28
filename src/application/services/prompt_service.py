"""
Единый сервис для управления промптами с Valkey кэшированием.
"""

from typing import Optional, List
from src.core.abstractions.cache import BaseCache
from src.core.logging import get_logger
from src.core.prompts.registry import PROMPTS

logger = get_logger(__name__)


class PromptService:
    """
    Единый сервис для управления промптами.
    Использует Valkey для кэширования с fallback к встроенным промптам.
    """

    def __init__(self, cache: BaseCache, ttl: int = 3600):
        """
        Инициализация сервиса промптов.

        :param cache: Кэш для хранения промптов
        :param ttl: Время жизни кэша в секундах (по умолчанию 1 час)
        """
        self.cache = cache
        self.ttl = ttl
        self.prompt_prefix = "prompt:"

    async def get_prompt(self, prompt_key: str) -> Optional[str]:
        """
        Получить промпт из кэша или fallback.

        :param prompt_key: Ключ промпта
        :return: Текст промпта или None
        """
        try:
            cache_key = f"{self.prompt_prefix}{prompt_key}"

            cached_prompt = await self.cache.get(cache_key)
            if cached_prompt:
                logger.debug(f"Промпт '{prompt_key}' загружен из кэша")
                return cached_prompt

            if prompt_key in PROMPTS:
                prompt = PROMPTS[prompt_key]
                await self.cache.set(cache_key, prompt, self.ttl)
                logger.info(f"Промпт '{prompt_key}' загружен из registry и закэширован")
                return prompt

            logger.warning(f"Промпт '{prompt_key}' не найден")
            return None

        except Exception as e:
            logger.error(f"Ошибка при получении промпта '{prompt_key}': {e}")
            return PROMPTS.get(prompt_key)

    async def set_prompt(self, prompt_key: str, prompt_text: str) -> bool:
        """
        Сохранить промпт в кэш.

        :param prompt_key: Ключ промпта
        :param prompt_text: Текст промпта
        :return: True если успешно сохранен
        """
        try:
            cache_key = f"{self.prompt_prefix}{prompt_key}"
            success = await self.cache.set(cache_key, prompt_text, self.ttl)

            if success:
                logger.info(f"Промпт '{prompt_key}' сохранен в кэш")
            else:
                logger.error(f"Не удалось сохранить промпт '{prompt_key}' в кэш")

            return success

        except Exception as e:
            logger.error(f"Ошибка при сохранении промпта '{prompt_key}': {e}")
            return False

    async def format_prompt(self, prompt_key: str, **kwargs) -> Optional[str]:
        """
        Получить и форматировать промпт с подстановкой параметров.

        :param prompt_key: Ключ промпта
        :param kwargs: Параметры для подстановки
        :return: Отформатированный промпт или None
        """
        try:
            prompt_template = await self.get_prompt(prompt_key)

            if not prompt_template:
                return None

            formatted_prompt = prompt_template.format(**kwargs)
            logger.debug(
                f"Промпт '{prompt_key}' отформатирован с параметрами: {list(kwargs.keys())}"
            )

            return formatted_prompt

        except KeyError as e:
            logger.error(
                f"Отсутствует параметр для форматирования промпта '{prompt_key}': {e}"
            )
            return None
        except Exception as e:
            logger.error(f"Ошибка при форматировании промпта '{prompt_key}': {e}")
            return None

    async def list_prompts(self) -> List[str]:
        """
        Получить список всех доступных промптов.

        :return: Список ключей промптов (встроенные + кэшированные)
        """
        try:
            builtin_prompts = set(PROMPTS.keys())

            pattern = f"{self.prompt_prefix}*"
            cached_keys = await self.cache.get_keys_by_pattern(pattern)
            cached_prompts = {
                key.replace(self.prompt_prefix, "") for key in cached_keys
            }

            all_prompts = list(builtin_prompts | cached_prompts)

            logger.info(
                f"Найдено {len(all_prompts)} промптов (встроенных: {len(builtin_prompts)}, кэшированных: {len(cached_prompts)})"
            )
            return all_prompts

        except Exception as e:
            logger.error(f"Ошибка при получении списка промптов: {e}")
            return list(PROMPTS.keys())

    async def delete_prompt(self, prompt_key: str) -> bool:
        """
        Удалить промпт из кэша.

        :param prompt_key: Ключ промпта
        :return: True если успешно удален
        """
        try:
            cache_key = f"{self.prompt_prefix}{prompt_key}"
            success = await self.cache.delete(cache_key)

            if success:
                logger.info(f"Промпт '{prompt_key}' удален из кэша")
            else:
                logger.warning(f"Промпт '{prompt_key}' не найден для удаления")

            return success

        except Exception as e:
            logger.error(f"Ошибка при удалении промпта '{prompt_key}': {e}")
            return False

    async def clear_all_prompts(self) -> bool:
        """
        Очистить все промпты из кэша (не влияет на встроенные).

        :return: True если успешно очищены
        """
        try:
            pattern = f"{self.prompt_prefix}*"
            keys = await self.cache.get_keys_by_pattern(pattern)

            if not keys:
                logger.info("Нет кэшированных промптов для очистки")
                return True

            deleted_count = 0
            for key in keys:
                if await self.cache.delete(key):
                    deleted_count += 1

            logger.info(f"Очищено {deleted_count} из {len(keys)} кэшированных промптов")
            return deleted_count == len(keys)

        except Exception as e:
            logger.error(f"Ошибка при очистке промптов: {e}")
            return False

    def get_prompt_sync(self, prompt_key: str) -> Optional[str]:
        """
        Синхронная версия получения промпта (только встроенные).

        :param prompt_key: Ключ промпта
        :return: Текст промпта или None
        """
        return PROMPTS.get(prompt_key)

    def format_prompt_sync(self, prompt_key: str, **kwargs) -> Optional[str]:
        """
        Синхронная версия форматирования промпта (только встроенные).

        :param prompt_key: Ключ промпта
        :param kwargs: Параметры для подстановки
        :return: Отформатированный промпт или None
        """
        try:
            prompt_template = PROMPTS.get(prompt_key)
            if not prompt_template:
                return None

            return prompt_template.format(**kwargs)

        except KeyError as e:
            logger.error(
                f"Отсутствует параметр для форматирования промпта '{prompt_key}': {e}"
            )
            return None
        except Exception as e:
            logger.error(f"Ошибка при форматировании промпта '{prompt_key}': {e}")
            return None
