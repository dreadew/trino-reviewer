import logging
from urllib.parse import urlparse

import trino
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class TrinoQueryInput(BaseModel):
    """Входные параметры для Trino tool."""

    query: str = Field(
        description="SQL запрос для выполнения (только SELECT, SHOW, DESCRIBE)"
    )


class TrinoTool(BaseTool):
    """
    LangGraph tool для безопасного выполнения read-only запросов к Trino.

    Поддерживает только следующие типы запросов:
    - SELECT (с ограничением LIMIT)
    - SHOW (TABLES, SCHEMAS, CATALOGS)
    - DESCRIBE (TABLE, SCHEMA)
    - EXPLAIN
    """

    name: str = "trino_query"
    description: str = (
        "Выполняет read-only SQL запросы к Trino database. "
        "Поддерживает SELECT (с автоматическим LIMIT), SHOW, DESCRIBE, EXPLAIN. "
        "Используй для получения информации о схеме, таблицах и статистике."
    )
    args_schema: type = TrinoQueryInput
    connection_url: str = Field(description="URL подключения к Trino")

    def __init__(self, connection_url: str, **kwargs):
        """
        Инициализация tool с URL подключения к Trino.

        :param connection_url: URL подключения в формате trino://user@host:port/catalog/schema
        """
        super().__init__(connection_url=connection_url, **kwargs)
        self._connection = None

    def _get_connection(self) -> trino.dbapi.Connection:
        """Получить подключение к Trino (с ленивой инициализацией)."""
        if self._connection is None or self._connection.closed:
            parsed_url = urlparse(self.connection_url)

            self._connection = trino.dbapi.connect(
                host=parsed_url.hostname,
                port=parsed_url.port or 8080,
                user=parsed_url.username or "analyst",
                catalog=(
                    parsed_url.path.split("/")[1]
                    if len(parsed_url.path.split("/")) > 1
                    else None
                ),
                schema=(
                    parsed_url.path.split("/")[2]
                    if len(parsed_url.path.split("/")) > 2
                    else None
                ),
                http_scheme="https" if parsed_url.scheme == "trinos" else "http",
            )

        return self._connection

    def _validate_query(self, query: str) -> str:
        """
        Валидация запроса на безопасность (только read-only операции).

        :param query: SQL запрос
        :return: Нормализованный запрос
        :raises ValueError: Если запрос не безопасен
        """
        query_upper = query.strip().upper()

        allowed_operations = ["SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"]

        if not any(query_upper.startswith(op) for op in allowed_operations):
            raise ValueError(
                f"Разрешены только read-only операции: {', '.join(allowed_operations)}"
            )

        forbidden_operations = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "CREATE",
            "ALTER",
            "TRUNCATE",
            "GRANT",
            "REVOKE",
            "CALL",
        ]

        for forbidden in forbidden_operations:
            if forbidden in query_upper:
                raise ValueError(f"Операция {forbidden} запрещена")

        if query_upper.startswith("SELECT") and "LIMIT" not in query_upper:
            query = query.strip().rstrip(";") + " LIMIT 100"

        return query

    def _run(self, query: str) -> str:
        """
        Выполнение SQL запроса к Trino.

        :param query: SQL запрос
        :return: Результат в текстовом формате
        """
        try:
            safe_query = self._validate_query(query)
            logger.info(f"Выполняется Trino запрос: {safe_query[:100]}...")

            connection = self._get_connection()
            cursor = connection.cursor()
            cursor.execute(safe_query)

            columns = (
                [desc[0] for desc in cursor.description] if cursor.description else []
            )
            rows = cursor.fetchall()

            if not rows:
                return "Запрос выполнен успешно, но результатов нет."

            result_lines = []
            if columns:
                result_lines.append(" | ".join(columns))
                result_lines.append("-" * len(result_lines[0]))

            for row in rows[:50]:
                row_str = " | ".join(
                    str(cell) if cell is not None else "NULL" for cell in row
                )
                result_lines.append(row_str)

            if len(rows) > 50:
                result_lines.append(f"... и еще {len(rows) - 50} строк")

            return "\n".join(result_lines)

        except Exception as e:
            error_msg = f"Ошибка выполнения Trino запроса: {str(e)}"
            logger.error(error_msg)
            return error_msg

    async def _arun(self, query: str) -> str:
        """Асинхронная версия (пока синхронная реализация)."""
        return self._run(query)


def create_trino_tool(connection_url: str) -> TrinoTool:
    """
    Фабричная функция для создания Trino tool.

    :param connection_url: URL подключения к Trino
    :return: Настроенный TrinoTool
    """
    return TrinoTool(connection_url=connection_url)


SCHEMA_ANALYSIS_QUERIES = {
    "show_catalogs": "SHOW CATALOGS",
    "show_schemas": "SHOW SCHEMAS FROM {catalog}",
    "show_tables": "SHOW TABLES FROM {catalog}.{schema}",
    "describe_table": "DESCRIBE {catalog}.{schema}.{table}",
    "table_stats": """
        SELECT 
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns 
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
    """,
    "table_row_counts": """
        SELECT 
            table_name,
            table_rows as estimated_rows
        FROM information_schema.tables 
        WHERE table_schema = '{schema}' 
        AND table_type = 'BASE TABLE'
    """,
}
