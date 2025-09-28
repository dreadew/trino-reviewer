import json
from typing import Any, Dict, List

from langchain.schema import HumanMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from src.core.abstractions.message_handler import BaseMessageHandler
from src.core.abstractions.workflow import BaseWorkflow
from src.core.config import config
from src.core.logging import get_logger
from src.core.models.base import DDLStatement, Query
from src.core.prompts.registry import PROMPTS
from src.core.types.agent import AgentState
from src.core.utils.json import safe_extract_json
from src.application.tools.trino_mcp_tool import create_trino_mcp_tool
from src.application.tools.performance_analyzer import create_performance_analysis_tool
from src.application.tools.schema_diff_tool import create_schema_diff_tool
from src.application.tools.data_lineage_tool import create_data_lineage_tool


class AnalyzeSchemaWorkflow(BaseWorkflow):
    """Workflow для анализа SQL запросов."""

    def __init__(self, message_handler: BaseMessageHandler):
        self.message_handler = message_handler
        self.graph = self._build_graph()
        self.logger = get_logger(__name__)

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
        Валидация схемы БД с помощью Trino tool.

        :param state: Состояние агента
        :return: Обновленное состояние с информацией о схеме
        """
        if not state.get("url"):
            self.logger.warning("URL подключения не указан, пропускаем валидацию схемы")
            state["schema_info"] = "URL подключения к БД не предоставлен"
            return state

        try:
            from src.core.config import config

            trino_tool = create_trino_mcp_tool(
                mcp_server_url=config.TRINO_MCP_SERVER_URL, connection_url=state["url"]
            )

            schema_info = []

            catalogs_result = trino_tool._run("SHOW CATALOGS")
            schema_info.append(f"=== КАТАЛОГИ ===\n{catalogs_result}")

            from urllib.parse import urlparse

            parsed_url = urlparse(state["url"])
            if len(parsed_url.path.split("/")) >= 3:
                catalog = parsed_url.path.split("/")[1]
                schema = parsed_url.path.split("/")[2]

                if catalog and schema:
                    tables_query = f"SHOW TABLES FROM {catalog}.{schema}"
                    tables_result = trino_tool._run(tables_query)
                    schema_info.append(
                        f"=== ТАБЛИЦЫ В {catalog}.{schema} ===\n{tables_result}"
                    )

            state["schema_info"] = "\n\n".join(schema_info)
            self.logger.info("Валидация схемы выполнена успешно")

        except Exception as e:
            self.logger.error(f"Ошибка при валидации схемы: {e}")
            state["schema_info"] = f"Ошибка подключения к БД: {str(e)}"

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
            json_response = safe_extract_json(state["response"])
            parsed_result = json.loads(json_response)

            state["result"] = parsed_result
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.error(f"Ошибка при парсинге JSON: {e}")
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
        base_prompt = PROMPTS["base_template"].format(
            url=url or "не указан", ddl=ddl, queries=queries
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
