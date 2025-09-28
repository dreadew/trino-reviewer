import json
from typing import Any, Dict, List

from langchain.schema import HumanMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from src.application.tools.data_lineage_tool import create_data_lineage_tool
from src.application.tools.performance_analyzer import create_performance_analysis_tool
from src.application.tools.schema_diff_tool import create_schema_diff_tool
from src.core.abstractions.message_handler import BaseMessageHandler
from src.core.abstractions.workflow import BaseWorkflow
from src.core.config import config
from src.core.logging import get_logger
from src.core.models.base import DDLStatement, Query
from src.core.prompts.registry import PROMPTS
from src.core.types.agent import AgentState
from src.core.utils.json import safe_extract_json


class AnalyzeSchemaWorkflow(BaseWorkflow):
    """Workflow для анализа SQL запросов."""

    def __init__(self, message_handler: BaseMessageHandler):
        self.message_handler = message_handler
        self.logger = get_logger(__name__)

        self.graph = self._build_graph()

    def _build_graph(self):
        graph = StateGraph(AgentState)
        graph.add_node("validate_schema", self._validate_schema_node)
        graph.add_node("analyze_performance", self._analyze_performance_node)
        graph.add_node("analyze_lineage", self._analyze_lineage_node)
        graph.add_node("compose_prompt", self._compose_prompt_node)
        graph.add_node("call_llm", self._call_llm_node)
        graph.add_node("parse_response", self._parse_response_node)
        graph.add_node("validate_changes", self._validate_changes_node)

        graph.add_edge(START, "validate_schema")
        graph.add_edge("validate_schema", "analyze_performance")
        graph.add_edge("analyze_performance", "analyze_lineage")
        graph.add_edge("analyze_lineage", "compose_prompt")
        graph.add_edge("compose_prompt", "call_llm")
        graph.add_edge("call_llm", "parse_response")
        graph.add_edge("parse_response", "validate_changes")
        graph.add_edge("validate_changes", END)

        return graph.compile(checkpointer=MemorySaver())

    def _validate_schema_node(self, state: AgentState) -> AgentState:
        """
        Валидация схемы БД с помощью Trino MCP HTTP клиента.

        :param state: Состояние агента
        :return: Обновленное состояние с информацией о схеме
        """
        if not state.get("url"):
            self.logger.warning("URL подключения не указан, пропускаем валидацию схемы")
            state["schema_info"] = "URL подключения к БД не предоставлен"
            return state

        try:
            import asyncio

            from src.application.clients.trino_mcp_http_client import TrinoMCPClient

            schema_info = []

            async def get_schema_info():
                async with TrinoMCPClient(
                    mcp_server_url=config.TRINO_MCP_SERVER_URL,
                    connection_url=state["url"],
                ) as client:
                    self.logger.info(
                        f"Подключение к Trino через MCP HTTP для URL: {state['url']}"
                    )

                    try:
                        status_result = await client.get_connection_status()
                        schema_info.append(
                            f"=== СТАТУС ПОДКЛЮЧЕНИЯ ===\n{status_result}"
                        )
                        self.logger.info("Получен статус подключения")

                        catalogs_result = await client.list_catalogs()
                        schema_info.append(
                            f"=== ДОСТУПНЫЕ КАТАЛОГИ ===\n{catalogs_result}"
                        )
                        self.logger.info("Получен список каталогов")

                        test_query_result = await client.execute_query(
                            "SELECT 1 as connection_test"
                        )
                        schema_info.append(
                            f"=== ТЕСТ ПОДКЛЮЧЕНИЯ ===\nРезультат: {test_query_result}"
                        )
                        self.logger.info("Тест подключения к Trino выполнен успешно")

                    except Exception as mcp_error:
                        self.logger.error(f"Ошибка при работе с MCP: {mcp_error}")
                        schema_info.append(f"=== ОШИБКА MCP ===\n{str(mcp_error)}")

            try:
                loop = asyncio.get_running_loop()
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, get_schema_info())
                    future.result(timeout=30.0)

            except RuntimeError:
                asyncio.run(get_schema_info())

            if schema_info:
                state["schema_info"] = "\n\n".join(schema_info)
                self.logger.info("Валидация схемы выполнена успешно")
            else:
                state["schema_info"] = (
                    "MCP HTTP клиент создан, но не удалось получить информацию о схеме"
                )
                self.logger.warning("Валидация схемы завершена без получения данных")

        except Exception as e:
            self.logger.error(f"Ошибка при валидации схемы: {e}")
            state["schema_info"] = f"Ошибка подключения к БД через MCP HTTP: {str(e)}"

        return state

    def _analyze_performance_node(self, state: AgentState) -> AgentState:
        """
        Анализ производительности SQL запросов.

        :param state: Состояние агента
        :return: Обновленное состояние с результатами анализа производительности
        """
        try:
            self.logger.info("Начат анализ производительности запросов")

            queries_data = []
            for query in state.get("queries", []):
                query_dict = {
                    "query_id": query.query_id,
                    "query": query.query,
                    "executiontime": query.executiontime,
                    "runquantity": query.runquantity,
                }
                queries_data.append(query_dict)

            if queries_data:
                performance_tool = create_performance_analysis_tool()
                performance_result = performance_tool._run(queries_data)
                state["performance_analysis"] = performance_result
                self.logger.info("Анализ производительности завершен успешно")
            else:
                state["performance_analysis"] = (
                    "Нет данных о запросах для анализа производительности"
                )

        except Exception as e:
            self.logger.error(f"Ошибка при анализе производительности: {e}")
            state["performance_analysis"] = (
                f"Ошибка анализа производительности: {str(e)}"
            )

        return state

    def _analyze_lineage_node(self, state: AgentState) -> AgentState:
        """
        Анализ зависимостей данных (data lineage).

        :param state: Состояние агента
        :return: Обновленное состояние с результатами анализа зависимостей
        """
        try:
            self.logger.info("Начат анализ зависимостей данных")

            sql_queries = [query.query for query in state.get("queries", [])]

            if sql_queries:
                lineage_tool = create_data_lineage_tool()
                lineage_result = lineage_tool._run(sql_queries)
                state["data_lineage"] = lineage_result
                self.logger.info("Анализ зависимостей данных завершен успешно")
            else:
                state["data_lineage"] = "Нет SQL запросов для анализа зависимостей"

        except Exception as e:
            self.logger.error(f"Ошибка при анализе зависимостей: {e}")
            state["data_lineage"] = f"Ошибка анализа зависимостей: {str(e)}"

        return state

    def _compose_prompt_node(self, state: AgentState) -> AgentState:
        """
        Создать промпт на основе состояния.

        :param state: Состояние агента
        :return: Обновленное состояние
        """
        prompt = self._compose_prompt(
            ddl=state["ddl"],
            queries=state["queries"],
            url=state.get("url"),
            schema_info=state.get("schema_info"),
            performance_analysis=state.get("performance_analysis"),
            data_lineage=state.get("data_lineage"),
        )
        state["prompt"] = prompt
        return state

    def _call_llm_node(self, state: AgentState) -> AgentState:
        messages = [SystemMessage(content=PROMPTS["system_reviewer"])]
        if state.get("chat_history"):
            for msg in state["chat_history"]:
                if msg["role"] == "user":
                    messages.append(HumanMessage(content=msg["content"]))
                elif msg["role"] == "assistant":
                    from langchain.schema import AIMessage

                    messages.append(AIMessage(content=msg["content"]))

        messages.append(HumanMessage(content=state["prompt"]))

        response = self.message_handler.process_messages(
            prompt=state["prompt"],
            system_message=PROMPTS["system_reviewer"],
            chat_history=state.get("chat_history", []),
        )

        self.logger.info("Ответ от LLM получен успешно")
        state["response"] = response

        self._update_chat_history(state)
        return state

    def _update_chat_history(self, state: AgentState) -> None:
        """
        Обновить историю чата с ограничением размера.

        :param state: Состояние агента
        """
        if "chat_history" not in state:
            state["chat_history"] = []

        state["chat_history"].append({"role": "user", "content": state["prompt"]})
        state["chat_history"].append(
            {"role": "assistant", "content": state["response"]}
        )

        max_size = config.MAX_CHAT_HISTORY_SIZE
        if len(state["chat_history"]) > max_size:
            state["chat_history"] = state["chat_history"][-max_size:]

        return state

    def _parse_response_node(self, state: AgentState) -> AgentState:
        try:
            self.logger.info(
                f"Парсинг ответа от LLM, длина: {len(state['response'])} символов"
            )
            self.logger.debug(f"Ответ от LLM: {state['response'][:500]}...")

            json_response = safe_extract_json(state["response"])
            self.logger.info(f"Извлечен JSON, длина: {len(json_response)} символов")

            parsed_result = json.loads(json_response)
            state["result"] = parsed_result
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            self.logger.error(f"Ошибка при парсинге JSON: {e}")
            self.logger.error(
                f"Неудачный текст для парсинга: {state['response'][:1000]}..."
            )
            raise
        return state

    def _validate_changes_node(self, state: AgentState) -> AgentState:
        """
        Валидация предложенных изменений схемы с помощью Schema Diff Tool.

        :param state: Состояние агента
        :return: Обновленное состояние с результатами валидации
        """
        try:
            self.logger.info("Начата валидация предложенных изменений")

            current_ddl = [stmt.statement for stmt in state.get("ddl", [])]

            result = state.get("result", {})
            proposed_ddl = []
            if result and "ddl" in result:
                proposed_ddl = [
                    ddl_item.get("statement", "")
                    for ddl_item in result["ddl"]
                    if ddl_item.get("statement")
                ]

            if current_ddl or proposed_ddl:
                diff_tool = create_schema_diff_tool()
                diff_result = diff_tool._run(current_ddl, proposed_ddl)
                state["schema_diff"] = diff_result
                self.logger.info("Валидация изменений завершена успешно")
            else:
                state["schema_diff"] = "Нет данных для сравнения схем"

        except Exception as e:
            self.logger.error(f"Ошибка при валидации изменений: {e}")
            state["schema_diff"] = f"Ошибка валидации изменений: {str(e)}"

        return state

    def _compose_prompt(
        self,
        ddl: List[DDLStatement],
        queries: List[Query],
        url: str = None,
        schema_info: str = None,
        performance_analysis: str = None,
        data_lineage: str = None,
    ) -> str:
        """
        Составить промпт для анализа схемы.

        :param ddl: DDL утверждения
        :param queries: SQL запросы
        :param url: URL подключения к БД
        :param schema_info: Информация о реальной схеме из БД
        :param performance_analysis: Результаты анализа производительности
        :param data_lineage: Результаты анализа зависимостей данных
        :return: Сформатированный промпт
        """
        ddl_statements = "\n".join([stmt.statement for stmt in ddl])

        queries_text = "\n".join(
            [
                f"Query ID: {query.query_id}\nQuery: {query.query}\nExecution Time: {query.executiontime}ms\nRun Quantity: {query.runquantity}"
                for query in queries
            ]
        )

        base_prompt = PROMPTS["trino_schema_analysis"].format(
            ddl_statements=ddl_statements, queries=queries_text
        )

        if schema_info:
            base_prompt += f"\n\nРЕАЛЬНАЯ ИНФОРМАЦИЯ О СХЕМЕ БД:\n{schema_info}\n"

        if performance_analysis:
            base_prompt += f"\n\nАНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ:\n{performance_analysis}\n"

        if data_lineage:
            base_prompt += f"\n\nАНАЛИЗ ЗАВИСИМОСТЕЙ ДАННЫХ:\n{data_lineage}\n"

        if schema_info or performance_analysis or data_lineage:
            base_prompt += "\nИспользуй эту дополнительную информацию для более точных рекомендаций."

        return base_prompt

    def execute(
        self, initial_state: Dict[str, Any], thread_id: str = None
    ) -> Dict[str, Any]:
        """
        Выполнить workflow анализа схемы.

        :param initial_state: Начальное состояние
        :param thread_id: ID треда (по умолчанию из конфигурации)
        :return: Результат выполнения
        """
        config_dict = {
            "configurable": {"thread_id": thread_id or config.DEFAULT_THREAD_ID},
        }
        final_state = self.graph.invoke(initial_state, config=config_dict)
        return final_state["result"]
