import asyncio
import json
from typing import Any, Dict, List
import aiohttp
from langchain_core.tools import BaseTool
from pydantic import Field

from src.core.logging import get_logger
from src.application.inputs.trino_mcp import TrinoMCPQueryInput

logger = get_logger(__name__)


class TrinoMCPClient:
    """Клиент для взаимодействия с Trino MCP сервером."""

    def __init__(self, mcp_server_url: str, connection_url: str):
        """
        Инициализация MCP клиента.

        :param mcp_server_url: URL MCP сервера (например, http://localhost:8000)
        :param connection_url: URL подключения к Trino
        """
        self.mcp_server_url = mcp_server_url.rstrip("/")
        self.connection_url = connection_url
        self.session = None

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Выполнить SQL запрос через MCP сервер.

        :param query: SQL запрос
        :return: Результат выполнения
        """
        if not self.session:
            raise RuntimeError("MCP клиент не инициализирован")

        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "execute_sql_query",
                    "arguments": {"jdbc_url": self.connection_url, "query": query},
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка выполнения запроса: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при выполнении запроса через MCP: {e}")
            raise

    async def get_connection_status(self) -> Dict[str, Any]:
        """
        Проверить статус подключения к Trino.

        :return: Статус подключения
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "connection_status",
                    "arguments": {"jdbc_url": self.connection_url},
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка проверки подключения: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при проверке подключения через MCP: {e}")
            raise

    async def list_catalogs(self) -> Dict[str, Any]:
        """
        Получить список каталогов.

        :return: Список каталогов
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "list_catalogs",
                    "arguments": {"jdbc_url": self.connection_url},
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка получения каталогов: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при получении каталогов через MCP: {e}")
            raise

    async def list_schemas(self, catalog: str) -> Dict[str, Any]:
        """
        Получить список схем в каталоге.

        :param catalog: Имя каталога
        :return: Список схем
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "list_schemas",
                    "arguments": {"jdbc_url": self.connection_url, "catalog": catalog},
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка получения схем: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при получении схем через MCP: {e}")
            raise

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> Dict[str, Any]:
        """
        Получить описание структуры таблицы.

        :param catalog: Каталог
        :param schema: Схема
        :param table: Таблица
        :return: Структура таблицы
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "describe_table",
                    "arguments": {
                        "jdbc_url": self.connection_url,
                        "catalog": catalog,
                        "schema": schema,
                        "table": table,
                    },
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка описания таблицы: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при описании таблицы через MCP: {e}")
            raise

    async def validate_ddl_statements(self, ddl_list: List[str]) -> Dict[str, Any]:
        """
        Валидировать DDL выражения без выполнения.

        :param ddl_list: Список DDL выражений
        :return: Результат валидации
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "validate_ddl_statements",
                    "arguments": {"ddl_list": ddl_list},
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка валидации DDL: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при валидации DDL через MCP: {e}")
            raise

    async def execute_ddl_statements(
        self,
        ddl_list: List[str],
        catalog: str,
        schema: str,
        validate_first: bool = True,
    ) -> Dict[str, Any]:
        """
        Выполнить DDL выражения с опциональной валидацией.

        :param ddl_list: Список DDL выражений
        :param catalog: Каталог
        :param schema: Схема
        :param validate_first: Валидировать перед выполнением
        :return: Результат выполнения
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "execute_ddl_statements",
                    "arguments": {
                        "jdbc_url": self.connection_url,
                        "ddl_list": ddl_list,
                        "catalog": catalog,
                        "schema": schema,
                        "validate_first": validate_first,
                    },
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка выполнения DDL: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при выполнении DDL через MCP: {e}")
            raise

    async def generate_schema_documentation(
        self, catalog: str, schema: str, include_ddl: List[str] = None
    ) -> Dict[str, Any]:
        """
        Сгенерировать документацию схемы.

        :param catalog: Каталог
        :param schema: Схема
        :param include_ddl: Дополнительные DDL для включения в документацию
        :return: Документация схемы
        """
        try:
            payload = {
                "method": "tools/call",
                "params": {
                    "name": "generate_schema_documentation",
                    "arguments": {
                        "jdbc_url": self.connection_url,
                        "catalog": catalog,
                        "schema": schema,
                        "include_ddl": include_ddl or [],
                    },
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                result = await response.json()

                if "error" in result:
                    raise Exception(f"Ошибка генерации документации: {result['error']}")

                return result.get("content", [{}])[0] if result.get("content") else {}

        except Exception as e:
            logger.error(f"Ошибка при генерации документации через MCP: {e}")
            raise


class TrinoMCPTool(BaseTool):
    """LangGraph tool для выполнения запросов к Trino через MCP сервер."""

    name: str = "trino_mcp_query"
    description: str = (
        "Выполняет SQL запросы к Trino database через MCP сервер. "
        "Поддерживает SELECT, SHOW, DESCRIBE, EXPLAIN запросы. "
        "Использует внешний MCP сервер для более надежной работы с Trino."
    )
    args_schema: type = TrinoMCPQueryInput
    mcp_server_url: str = Field(description="URL MCP сервера")
    connection_url: str = Field(description="URL подключения к Trino")

    def __init__(self, mcp_server_url: str, connection_url: str, **kwargs):
        """
        Инициализация tool с MCP сервером.

        :param mcp_server_url: URL MCP сервера
        :param connection_url: URL подключения к Trino
        """
        super().__init__(
            mcp_server_url=mcp_server_url, connection_url=connection_url, **kwargs
        )

    def _run(self, query: str) -> str:
        """
        Синхронное выполнение запроса.

        :param query: SQL запрос
        :return: Результат в виде строки
        """
        return asyncio.run(self._arun(query))

    async def _arun(self, query: str) -> str:
        """
        Асинхронное выполнение запроса.

        :param query: SQL запрос
        :return: Результат в виде строки
        """
        try:
            async with TrinoMCPClient(
                self.mcp_server_url, self.connection_url
            ) as client:
                result = await client.execute_query(query)

                if isinstance(result, dict):
                    if "text" in result:
                        return result["text"]
                    elif "results" in result:
                        results = result["results"]
                        if not results:
                            return "Запрос выполнен успешно, но результат пуст"

                        if isinstance(results, list) and len(results) > 0:
                            if isinstance(results[0], dict):
                                formatted_result = f"Найдено {len(results)} записей:\n"
                                for i, row in enumerate(results[:10]):
                                    formatted_result += f"{i+1}. {row}\n"
                                if len(results) > 10:
                                    formatted_result += (
                                        f"... и еще {len(results) - 10} записей"
                                    )
                                return formatted_result
                            else:
                                return (
                                    f"Результат: {', '.join(map(str, results[:20]))}"
                                    + ("..." if len(results) > 20 else "")
                                )

                    elif "data" in result:
                        return f"Данные получены: {result['data']}"

                    elif "message" in result:
                        return result["message"]

                    elif "status" in result:
                        return f"Статус: {result['status']}" + (
                            f", сообщение: {result.get('message', '')}"
                            if result.get("message")
                            else ""
                        )

                    else:
                        return json.dumps(result, indent=2, ensure_ascii=False)

                return str(result)

        except Exception as e:
            error_msg = f"Ошибка выполнения запроса через MCP: {str(e)}"
            logger.error(error_msg)
            return error_msg


def create_trino_mcp_tool(mcp_server_url: str, connection_url: str) -> TrinoMCPTool:
    """
    Фабричная функция для создания Trino MCP tool.

    :param mcp_server_url: URL MCP сервера
    :param connection_url: URL подключения к Trino
    :return: Настроенный TrinoMCPTool
    """
    return TrinoMCPTool(mcp_server_url=mcp_server_url, connection_url=connection_url)
