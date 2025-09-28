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
        template = """Отвечайте на вопросы как можно лучше. У вас есть доступ к следующим инструментам:

{tools}

Используйте следующий формат:

Вопрос: входной вопрос, на который вы должны ответить
Размышление: вы всегда должны думать о том, что делать
Действие: действие, которое нужно выполнить, должно быть одним из [{tool_names}]
Входные данные действия: входные данные для действия
Наблюдение: результат действия
... (это Размышление/Действие/Входные данные действия/Наблюдение может повторяться N раз)
Размышление: теперь я знаю окончательный ответ
Окончательный ответ: окончательный ответ на исходный вопрос

Начнем!

Вопрос: Вы - эксперт по анализу баз данных и SQL, специализирующийся на Trino/Presto. Ваша задача - проанализировать предоставленную схему базы данных и SQL запросы, предоставив конкретные рекомендации по оптимизации.

Вам предоставлены:
- DDL выражения схемы: {ddl}
- SQL запросы для анализа: {queries}
- URL подключения к БД: {url}

Требования:
- Анализируйте предоставленную схему и запросы
- Предоставьте конкретные рекомендации по оптимизации
- Объясните свои выводы простым языком
- Учитывайте специфику Trino/Presto

Пожалуйста, проведите анализ и предоставьте ваши рекомендации.

Пожалуйста, предоставьте детальный анализ включающий:
1. **Структура таблиц**: Оцените дизайн таблиц, индексы, ключи
2. **Производительность запросов**: Выявите потенциальные узкие места
3. **Рекомендации по оптимизации**: Конкретные предложения по улучшению
4. **Потенциальные проблемы**: Возможные проблемы с производительностью или целостностью данных

Дайте практические рекомендации учитывая специфику Trino/Presto и работу с большими объёмами данных.

**ВАЖНЫЕ ПРАВИЛА для Trino:**
- Все команды работы с таблицами должны использовать полный путь к таблице в формате <каталог>.<схема>.<таблица>
- В вашем ответе первой DDL командой должна идти команда создания новой схемы в этом же каталоге!
- Пример: CREATE SCHEMA data.NewSchema
- Все SQL запросы, которые переносят данные в новую структуру также должны придерживаться этого правила полной идентификации таблиц
- Пример: INSERT INTO catalog.myschema.h_authors SELECT * FROM catalog.public.h_authors
- Все запросы к новой структуре данных должны также указывать полный путь в новой схеме
- Пример: SELECT a.Col1, a.Col2, b.Col4 FROM catalog.myschema.MyTable1 as a JOIN catalog.myschema.MyTable2 as b on a.ID=b.ID
- query_id в твоем ответе должен совпадать с соответствующими query_id запросов из входных данных

**ФОРМАТЫ ВХОДНЫХ ДАННЫХ ДЛЯ ИНСТРУМЕНТОВ:**

1. **data_lineage** - Анализ зависимостей таблиц:
   - Вход: {{"queries": ["SELECT * FROM table1 JOIN table2 ON ...", "SELECT * FROM table3 WHERE ..."]}}
   - queries: список строк с SQL запросами

2. **performance_analyzer** - Анализ производительности:
   - Вход: {{"queries": [{{"query_id": "q1", "query": "SELECT ...", "runquantity": 100, "executiontime": 500}}]}}
   - queries: список словарей с полями query_id, query, runquantity, executiontime

3. **schema_diff** - Сравнение схем:
   - Вход: {{"current_schema": ["CREATE TABLE t1 ..."], "proposed_schema": ["CREATE TABLE t1 ...", "CREATE INDEX ..."]}}
   - current_schema: список строк DDL текущей схемы
   - proposed_schema: список строк DDL новой схемы

**ВНИМАНИЕ!** Всегда передавайте данные инструментам в правильном формате JSON. Не оборачивайте входные данные в дополнительные кавычки или текст.

**ВНИМАНИЕ!** Ответ должен быть строго в формате JSON, пример:
```json
{{
  "ddl": [{{"statement": "CREATE INDEX idx_example ON table_name (column_name)"}}],
  "migrations": [{{"statement": "ALTER TABLE table_name ADD COLUMN new_column VARCHAR(100)"}}],
  "queries": [{{"query_id": "optimized-query-1", "query": "SELECT * FROM table_name WHERE condition"}}]
}}
```
Все рекомендации и анализ должны быть структурированы в соответствующих полях.

Используйте инструменты по мере необходимости для сбора дополнительной информации.

{agent_scratchpad}"""

        return PromptTemplate.from_template(template)

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

            self.logger.info(f"Запуск ReAct агента для анализа схемы")

            result = self.executor.invoke(agent_input)

            # Парсим JSON ответ от агента
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
            # Ищем JSON в ответе (может быть обернут в markdown или текст)
            json_match = re.search(r"```json\s*(.*?)\s*```", response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Пробуем найти JSON без обертки
                json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
                json_str = json_match.group(0) if json_match else response_text

            # Парсим JSON
            parsed = json.loads(json_str)

            # Валидируем структуру
            if not isinstance(parsed, dict):
                raise ValueError("Ответ должен быть объектом JSON")

            # Проверяем наличие обязательных полей
            required_fields = ["ddl", "migrations", "queries"]
            for field in required_fields:
                if field not in parsed:
                    parsed[field] = []

            return parsed

        except (json.JSONDecodeError, AttributeError, ValueError) as e:
            self.logger.warning(f"Не удалось распарсить JSON ответ агента: {e}")
            # Возвращаем структуру с ошибкой
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
