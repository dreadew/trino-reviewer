import json
import uuid
from typing import Any, Dict, List

import aiohttp

from src.core.logging import get_logger

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
        self.session_id = None  # str(uuid.uuid4())

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        await self._initialize_mcp_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def _initialize_mcp_session(self):
        """Инициализация MCP сессии."""
        logger.info(
            f"Инициализация MCP сессии с начальным session_id: {self.session_id}"
        )
        try:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "clientInfo": {"name": "VkHackRecSys", "version": "1.0.0"},
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                },
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    logger.warning(f"MCP initialization failed: {error_text}")
                    return

                content_type = response.headers.get("content-type", "").lower()

                self.session_id = response.headers.get("mcp-session-id")
                if not self.session_id:
                    raise RuntimeError(
                        "MCP сервер не вернул mcp-session-id в заголовке"
                    )

                if "application/json" in content_type:
                    _ = await response.json()
                    await self._send_notification("notifications/initialized", {})
                    logger.info(
                        f"MCP session initialized successfully: {self.session_id}"
                    )
                elif "text/event-stream" in content_type:
                    _ = await self._parse_sse_response(response)
                    await self._send_notification("notifications/initialized", {})
                    logger.info(
                        f"MCP session initialized successfully via SSE: {self.session_id}"
                    )
                else:
                    raise RuntimeError(
                        f"Неожиданный content-type от MCP сервера: {content_type}"
                    )

        except Exception as e:
            logger.warning(f"MCP session initialization failed: {e}")
            raise

    async def _parse_sse_response(self, response) -> Dict[str, Any]:
        """Парсинг SSE ответа от MCP сервера."""
        async for line in response.content:
            line = line.decode("utf-8").strip()
            if line.startswith("data: "):
                data = line[6:]
                if data:
                    try:
                        return json.loads(data)
                    except json.JSONDecodeError:
                        continue
        raise RuntimeError("Не удалось разобрать SSE ответ от MCP сервера")

    async def _make_mcp_request(
        self,
        method: str,
        tool_name: str,
        arguments: Dict[str, Any],
        request_id: Any = None,
    ) -> Dict[str, Any]:
        """Базовый метод для выполнения MCP запросов."""
        if not self.session or not self.session_id:
            raise RuntimeError(
                "MCP клиент не инициализирован или session_id отсутствует"
            )

        logger.info(
            f"Отправка MCP запроса с session_id: {self.session_id}, method: {method}, tool: {tool_name}"
        )

        try:
            payload = {
                "jsonrpc": "2.0",
                "id": request_id or str(uuid.uuid4()),
                "method": method,
                "params": {
                    "name": tool_name,
                    "arguments": arguments,
                },
            }

            async with self.session.post(
                f"{self.mcp_server_url}/mcp",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                    "mcp-session-id": self.session_id,
                },
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:

                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(
                        f"MCP сервер вернул ошибку {response.status}: {error_text}"
                    )

                content_type = response.headers.get("content-type", "").lower()

                if "application/json" in content_type:
                    result = await response.json()
                elif "text/event-stream" in content_type:
                    result = await self._parse_sse_response(response)
                else:
                    resp_text = await response.text()
                    raise RuntimeError(
                        f"Неожиданный content-type от MCP сервера: {content_type}. Ответ: {resp_text}"
                    )

                if "error" in result:
                    raise Exception(f"Ошибка MCP: {result['error']}")

                return result.get("result", {})

        except Exception as e:
            logger.error(f"Ошибка при выполнении MCP запроса: {e}")
            raise

    async def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Выполнить SQL запрос через MCP сервер.

        :param query: SQL запрос
        :return: Результат выполнения
        """
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="execute_query_tool",
            arguments={"jdbc_url": self.connection_url, "sql": query},
        )

    async def get_connection_status(self) -> Dict[str, Any]:
        """
        Проверить статус подключения к Trino.

        :return: Статус подключения
        """
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="connection_status_tool",
            arguments={"jdbc_url": self.connection_url},
        )

    async def list_catalogs(self) -> Dict[str, Any]:
        """
        Получить список каталогов.

        :return: Список каталогов
        """
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="list_catalogs_tool",
            arguments={"jdbc_url": self.connection_url},
        )

    async def list_schemas(self, catalog: str) -> Dict[str, Any]:
        """
        Получить список схем в каталоге.

        :param catalog: Имя каталога
        :return: Список схем
        """
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="list_schemas_tool",
            arguments={"jdbc_url": self.connection_url, "catalog": catalog},
        )

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
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="describe_table_tool",
            arguments={
                "jdbc_url": self.connection_url,
                "catalog": catalog,
                "schema": schema,
                "table": table,
            },
        )

    async def validate_ddl_statements(self, ddl_list: List[str]) -> Dict[str, Any]:
        """
        Валидировать DDL выражения без выполнения.

        :param ddl_list: Список DDL выражений
        :return: Результат валидации
        """
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="validate_ddl_statements_tool",
            arguments={"ddl_list": ddl_list},
        )

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
        return await self._make_mcp_request(
            method="tools/call",
            tool_name="execute_ddl_statements_tool",
            arguments={
                "jdbc_url": self.connection_url,
                "ddl_list": ddl_list,
                "catalog": catalog,
                "schema": schema,
                "validate_first": validate_first,
            },
        )

    async def _send_notification(self, method: str, params: Dict[str, Any]):
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }
        async with self.session.post(
            f"{self.mcp_server_url}/mcp",
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "mcp-session-id": self.session_id,
            },
        ) as response:
            if response.status != 200:
                logger.warning(f"Notification {method} failed: {await response.text()}")

    async def generate_schema_documentation(
        self, catalog: str, schema: str, include_ddl: List[str] = None
    ) -> Dict[str, Any]:
        """
        Сгенерировать документацию схемы (заглушка - инструмент не реализован на сервере).

        :param catalog: Каталог
        :param schema: Схема
        :param include_ddl: Дополнительные DDL для включения в документацию
        :return: Документация схемы
        """
        return {
            "message": "Инструмент generate_schema_documentation не реализован на сервере",
            "catalog": catalog,
            "schema": schema,
            "include_ddl": include_ddl or [],
        }
