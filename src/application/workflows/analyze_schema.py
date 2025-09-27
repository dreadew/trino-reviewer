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
from src.core.prompts.base import BASE_PROMPT_TEMPLATE
from src.core.prompts.system import SYSTEM_REVIEWER_PROMPT
from src.core.types.agent import AgentState
from src.core.utils.json import safe_extract_json


class AnalyzeSchemaWorkflow(BaseWorkflow):
    """Workflow для анализа SQL запросов."""

    def __init__(self, message_handler: BaseMessageHandler):
        self.message_handler = message_handler
        self.graph = self._build_graph()
        self.logger = get_logger(__name__)

    def _build_graph(self):
        graph = StateGraph(AgentState)
        graph.add_node("compose_prompt", self._compose_prompt_node)
        graph.add_node("call_llm", self._call_llm_node)
        graph.add_node("parse_response", self._parse_response_node)

        graph.add_edge(START, "compose_prompt")
        graph.add_edge("compose_prompt", "call_llm")
        graph.add_edge("call_llm", "parse_response")
        graph.add_edge("parse_response", END)

        return graph.compile(checkpointer=MemorySaver())

    def _compose_prompt_node(self, state: AgentState) -> AgentState:
        """
        Создать промпт на основе состояния.

        :param state: Состояние агента
        :return: Обновленное состояние
        """
        prompt = self._compose_prompt(
            ddl=state["ddl"], queries=state["queries"], url=state.get("url")
        )
        state["prompt"] = prompt
        return state

    def _call_llm_node(self, state: AgentState) -> AgentState:
        messages = [SystemMessage(content=SYSTEM_REVIEWER_PROMPT)]
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
            system_message=SYSTEM_REVIEWER_PROMPT,
            chat_history=state.get("chat_history", []),
        )

        self.logger.info("LLM response received successfully")
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

    def _compose_prompt(
        self, ddl: List[DDLStatement], queries: List[Query], url: str = None
    ) -> str:
        """
        Составить промпт для анализа схемы.

        :param ddl: DDL утверждения
        :param queries: SQL запросы
        :param url: URL подключения к БД
        :return: Сформатированный промпт
        """
        return BASE_PROMPT_TEMPLATE.format(
            url=url or "не указан", ddl=ddl, queries=queries
        )

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
