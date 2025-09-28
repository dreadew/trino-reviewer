import json
import re
from typing import Any, Dict, List, Optional

from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import BaseTool

from src.application.tools.data_lineage_tool import create_data_lineage_tool
from src.application.tools.performance_analyzer import create_performance_analysis_tool
from src.application.tools.schema_diff_tool import create_schema_diff_tool
from src.core.abstractions.agent import BaseAgent
from src.core.abstractions.chat_model import ChatModel
from src.core.logging import get_logger
from src.core.prompts.registry import PROMPTS
from src.infra.langfuse import callback_handler


class ReactAgent(BaseAgent):
    """ReAct агент для анализа схем БД с динамическими инструментами."""

    def __init__(
        self,
        model: ChatModel,
        tools: Optional[List[BaseTool]] = None,
    ):
        """
        Инициализация ReAct агента.

        :param model: Модель чата
        :param tools: Список инструментов (опционально, по умолчанию все доступные)
        """
        super().__init__(model)
        self.logger = get_logger(__name__)

        self.available_tools = self._create_available_tools()

        self.tools = tools or list(self.available_tools.values())

        self.prompt = self._create_prompt()

        self.agent = create_react_agent(
            llm=self.model.langchain_model, tools=self.tools, prompt=self.prompt
        )

        self.executor = AgentExecutor.from_agent_and_tools(
            agent=self.agent,
            tools=self.tools,
            verbose=True,
            handle_parsing_errors=True,
        )

    def _create_available_tools(self) -> Dict[str, BaseTool]:
        """Создать все доступные инструменты."""
        return {
            "schema_diff": create_schema_diff_tool(),
            "data_lineage": create_data_lineage_tool(),
            "performance_analysis": create_performance_analysis_tool(),
        }

    def _create_prompt(self) -> PromptTemplate:
        """Создать prompt для ReAct агента."""
        return PromptTemplate.from_template(PROMPTS["react_agent"])

    def add_tool(self, tool: BaseTool) -> None:
        """
        Динамически добавить инструмент.

        :param tool: Инструмент для добавления
        """
        if tool not in self.tools:
            self.tools.append(tool)
            self.executor = AgentExecutor.from_agent_and_tools(
                agent=self.agent,
                tools=self.tools,
                verbose=True,
                handle_parsing_errors=True,
            )
            self.logger.info(f"Добавлен инструмент: {tool.name}")

    def remove_tool(self, tool_name: str) -> None:
        """
        Удалить инструмент по имени.

        :param tool_name: Имя инструмента для удаления
        """
        self.tools = [tool for tool in self.tools if tool.name != tool_name]
        if len(self.tools) > 0:
            self.executor = AgentExecutor.from_agent_and_tools(
                agent=self.agent,
                tools=self.tools,
                verbose=True,
                handle_parsing_errors=True,
            )
        self.logger.info(f"Удален инструмент: {tool_name}")

    def review(
        self,
        payload: Dict[str, Any],
        thread_id: str = None,
    ) -> Dict[str, Any]:
        """
        Выполнить анализ схемы с помощью ReAct агента.

        :param payload: Словарь с ключами 'ddl', 'queries', 'url'
        :param thread_id: Идентификатор треда
        :return: Результат анализа
        """
        try:
            validation_result = self._validate_payload(payload)
            if "error" in validation_result:
                return validation_result

            ddl_text = "\n".join([str(stmt) for stmt in payload["ddl"]])
            queries_text = "\n".join([str(query) for query in payload["queries"]])

            agent_input = {
                "ddl": ddl_text,
                "queries": queries_text,
                "url": payload["url"],
            }

            self.logger.info("Запуск ReAct агента для анализа схемы")

            result = self.executor.invoke(
                agent_input, {"callbacks": [callback_handler]}
            )

            output_text = result.get("output", "")
            parsed_result = self._parse_agent_response(output_text)

            return {
                "result": parsed_result,
                "intermediate_steps": result.get("intermediate_steps", []),
                "chat_history": [],
            }

        except Exception as e:
            self.logger.error(f"Ошибка при анализе схемы: {e}")
            return {"error": "Внутренняя ошибка при анализе схемы", "details": str(e)}

    def _parse_agent_response(self, response_text: str) -> Dict[str, Any]:
        """
        Парсинг JSON ответа от агента.

        :param response_text: Текст ответа от агента
        :return: Распарсенный JSON или текст ошибки
        """
        try:
            json_match = re.search(r"```json\s*(.*?)\s*```", response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
                json_str = json_match.group(0) if json_match else response_text

            parsed = json.loads(json_str)

            if not isinstance(parsed, dict):
                raise ValueError("Ответ должен быть объектом JSON")

            required_fields = ["ddl", "migrations", "queries"]
            for field in required_fields:
                if field not in parsed:
                    parsed[field] = []

            return parsed

        except (json.JSONDecodeError, AttributeError, ValueError) as e:
            self.logger.warning(f"Не удалось распарсить JSON ответ агента: {e}")
            return {
                "error": "Не удалось распарсить ответ агента",
                "raw_response": response_text,
                "ddl": [],
                "migrations": [],
                "queries": [],
            }

    def _validate_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Валидация входных данных.

        :param payload: Входные данные
        :return: Результат валидации
        """
        required_keys = ["url", "ddl", "queries"]
        missing_keys = [key for key in required_keys if key not in payload]
        if missing_keys:
            return {
                "error": "Отсутствуют обязательные ключи",
                "details": f"Отсутствующие ключи: {missing_keys}",
            }

        if not isinstance(payload.get("url"), str) or not payload["url"]:
            return {
                "error": "Неверный формат URL",
                "details": "URL должен быть строкой и не должен быть пустым",
            }

        if not isinstance(payload.get("ddl"), list):
            return {
                "error": "Неверный формат DDL",
                "details": "DDL должен быть списком",
            }

        if not isinstance(payload.get("queries"), list):
            return {
                "error": "Неверный формат запросов",
                "details": "Queries должен быть списком",
            }

        return {"valid": True}
