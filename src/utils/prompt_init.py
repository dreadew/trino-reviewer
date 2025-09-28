import asyncio
from src.core.factories.service_factory import service_factory
from src.core.logging import get_logger

logger = get_logger(__name__)

DEFAULT_PROMPTS = {
    "trino_schema_analysis": """Задача: Анализ и оптимизация схемы БД Trino

Входные данные:
- Строка подключения: {url}
- DDL схемы: {ddl}
- SQL запросы с метриками: {queries}

Контекст:
Ты - эксперт по оптимизации схем данных для Trino (distributed SQL query engine).
Анализируешь текущую схему БД и SQL запросы с их метриками производительности.

Метрики запросов:
- runquantity: количество выполнений запроса
- executiontime: среднее время выполнения в миллисекундах

Анализ:
1) Изучи DDL схемы и поймите структуру данных
2) Проанализируй SQL запросы и их частоту выполнения
3) Выяви узкие места производительности на основе метрик
4) Оцени необходимость создания индексов, партиционирования или денормализации
5) Учти специфику Trino: columnar storage, distributed queries, predicate pushdown

Рекомендации по оптимизации:
- Создание материализованных представлений для частых запросов
- Партиционирование таблиц по часто используемым столбцам в WHERE
- Денормализация для запросов с высоким временем выполнения
- Оптимизация JOIN операций
- Создание индексов для фильтрации

Формат ответа (строго JSON):
{{
  "ddl": [{{"statement": "DDL команда"}}],
  "migrations": [{{"statement": "Миграция данных"}}],
  "queries": [{{"query_id": "ID", "query": "Оптимизированный запрос"}}]
}}

Требования:
- Если оптимизация не нужна, верни пустые массивы
- Сохраняй query_id из входных данных
- Используй полный путь: catalog.schema.table
- Первой DDL командой создавай схему если нужно
- Только валидный JSON, никаких комментариев""",
    "system_reviewer": """Ты — экспертный ревьюер схем БД и SQL запросов.
Ответ должен быть строго в JSON — НИЧЕГО кроме валидного JSON.""",
    "performance_analysis_prompt": """Анализируй метрики производительности SQL запросов:

Данные для анализа:
{queries_data}

Найди:
1. Самые медленные запросы (execution_time > 1000мс)
2. Наиболее частые запросы (run_quantity > 1000)
3. Запросы с наибольшим суммарным временем
4. Потенциальные узкие места

Рекомендации должны касаться ТОЛЬКО схемы БД:
- Создание индексов
- Партиционирование таблиц
- Материализованные представления
- Денормализация

НЕ ИЗМЕНЯЙ SQL запросы! Только схему БД.""",
    "schema_diff_prompt": """Сравни две схемы БД и найди различия:

Текущая схема:
{current_schema}

Новая схема:
{proposed_schema}

Определи:
1. Какие объекты добавлены
2. Какие объекты удалены  
3. Какие объекты изменены
4. Breaking changes
5. Безопасные миграции

Формат ответа - список изменений с описанием влияния.""",
    "data_lineage_prompt": """Проанализируй зависимости данных в SQL запросах:

SQL запросы:
{queries}

Построй граф зависимостей:
1. Какие таблицы используются
2. Как таблицы связаны через JOIN
3. Какие таблицы являются источниками данных
4. Какие таблицы наиболее критичны
5. Потенциальные cascade эффекты при изменениях

Выдели критические пути данных и таблицы-узкие места.""",
}


async def init_prompts_in_valkey():
    """
    Инициализировать стандартные промпты в Valkey кэше.
    """
    logger.info("Начинаем инициализацию промптов в Valkey...")

    try:
        prompt_service = service_factory.create_prompt_service()

        success_count = 0
        for prompt_key, prompt_text in DEFAULT_PROMPTS.items():
            try:
                success = await prompt_service.set_prompt(prompt_key, prompt_text)
                if success:
                    success_count += 1
                    logger.info(f"Промпт '{prompt_key}' загружен в Valkey")
                else:
                    logger.error(f"Не удалось загрузить промпт '{prompt_key}'")

            except Exception as e:
                logger.error(f"Ошибка при загрузке промпта '{prompt_key}': {e}")

        logger.info(
            f"Инициализация завершена: {success_count}/{len(DEFAULT_PROMPTS)} промптов загружено"
        )
        return success_count == len(DEFAULT_PROMPTS)

    except Exception as e:
        logger.error(f"Ошибка при инициализации промптов: {e}")
        return False


async def list_prompts_in_valkey():
    """
    Показать все промпты в Valkey кэше.
    """
    try:
        prompt_service = service_factory.create_prompt_service()
        prompts = await prompt_service.list_prompts()

        if prompts:
            logger.info(f"Найдено {len(prompts)} промптов в Valkey:")
            for prompt_key in prompts:
                prompt_content = await prompt_service.get_prompt(prompt_key)
                if prompt_content:
                    size = len(prompt_content)
                    logger.info(f"- {prompt_key}: {size} символов")
                else:
                    logger.info(f"- {prompt_key}: содержимое недоступно")
        else:
            logger.info("Промпты в Valkey не найдены")

        return prompts

    except Exception as e:
        logger.error(f"Ошибка при получении списка промптов: {e}")
        return []


async def clear_prompts_in_valkey():
    """
    Очистить все промпты из Valkey кэша.
    """
    try:
        prompt_service = service_factory.create_prompt_service()
        success = await prompt_service.clear_all_prompts()

        if success:
            logger.info("Все промпты очищены из Valkey")
        else:
            logger.warning("Не все промпты были очищены")

        return success

    except Exception as e:
        logger.error(f"Ошибка при очистке промптов: {e}")
        return False


async def main():
    """
    Главная функция для управления промптами.
    """
    import sys

    if len(sys.argv) < 2:
        print("Использование:")
        print(
            "  python -m src.utils.prompt_init init    - Загрузить стандартные промпты"
        )
        print("  python -m src.utils.prompt_init list    - Показать все промпты")
        print("  python -m src.utils.prompt_init clear   - Очистить все промпты")
        return

    command = sys.argv[1].lower()

    if command == "init":
        success = await init_prompts_in_valkey()
        sys.exit(0 if success else 1)
    elif command == "list":
        await list_prompts_in_valkey()
    elif command == "clear":
        success = await clear_prompts_in_valkey()
        sys.exit(0 if success else 1)
    else:
        print(f"Неизвестная команда: {command}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
