from typing import Any, Dict, List, Optional

from src.core.abstractions.agent import BaseAgent
from src.core.abstractions.chat_model import ChatModel
from src.core.abstractions.llm import BaseLLMService
from src.core.abstractions.workflow import BaseWorkflow
from src.core.logging import get_logger
from src.core.models.base import DDLStatement, Query


class SchemaReviewerAgent(BaseAgent):
    """Агент для анализа схемы БД."""

    def __init__(
        self,
        model: Optional[ChatModel] = None,
        llm_service: BaseLLMService = None,
        workflow: BaseWorkflow = None,
    ):
        super().__init__(model)
        self.llm_service = llm_service
        self.workflow = workflow
        self.logger = get_logger(__name__)

    def review(
        self,
        payload: Dict[str, Any],
        thread_id: str = None,
    ) -> Dict[str, Any]:
        """
        Анализ схемы БД на основе DDL и SQL запросов.

        :param payload: Словарь с ключами 'ddl' и 'queries'
        :param thread_id: Идентификатор треда
        :return: Результат анализа
        """
        try:
            parsed_data = self._parse_and_validate_payload(payload)
            if "error" in parsed_data:
                return parsed_data

            initial_state = {
                "ddl": parsed_data["ddl"],
                "queries": parsed_data["queries"],
                "url": parsed_data["url"],
                "prompt": "",
                "response": "",
                "result": {},
                "chat_history": [],
            }

            return self.workflow.execute(initial_state, thread_id)

        except Exception as e:
            self.logger.error(f"Ошибка при анализе схемы: {e}")
            return {"error": "Внутренняя ошибка при анализе схемы", "details": str(e)}

    def _parse_and_validate_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Парсинг и валидация входных данных.

        :param payload: Входные данные
        :return: Распарсенные и валидированные данные или ошибка
        """
        required_keys = ["url", "ddl", "queries"]
        missing_keys = [key for key in required_keys if key not in payload]
        if missing_keys:
            return {
                "error": "Отсутствуют обязательные ключи",
                "details": f"Отсутствующие ключи: {missing_keys}",
            }

        try:
            url_data = payload.get("url")
            if not isinstance(url_data, str) or not url_data:
                return {
                    "error": "Неверный формат URL",
                    "details": "URL должен быть строкой и не должен быть пустым",
                }

            ddl_data = payload["ddl"]
            if not isinstance(ddl_data, list):
                return {
                    "error": "Неверный формат DDL",
                    "details": "DDL должен быть списком",
                }

            parsed_ddl = []
            for i, item in enumerate(ddl_data):
                if isinstance(item, dict) and "statement" in item:
                    parsed_ddl.append(DDLStatement(statement=item["statement"]))
                elif isinstance(item, str):
                    parsed_ddl.append(DDLStatement(statement=item))
                else:
                    return {
                        "error": f"Неверный элемент DDL на индексе {i}",
                        "details": "Элементы DDL должны иметь поле 'statement' или быть строками",
                    }

            queries_data = payload["queries"]
            if not isinstance(queries_data, list):
                return {
                    "error": "Неверный формат запросов",
                    "details": "Запросы должны быть списком",
                }

            parsed_queries = []
            for i, item in enumerate(queries_data):
                if not isinstance(item, dict):
                    return {
                        "error": f"Неверный элемент запроса на индексе {i}",
                        "details": "Элементы запроса должны быть словарями",
                    }

                required_query_fields = [
                    "query_id",
                    "query",
                    "runquantity",
                    "executiontime",
                ]
                missing_fields = [
                    field for field in required_query_fields if field not in item
                ]
                if missing_fields:
                    return {
                        "error": f"Неверный запрос на индексе {i}",
                        "details": f"Отсутствующие поля: {missing_fields}",
                    }

                try:
                    parsed_queries.append(
                        Query(
                            query_id=str(item["query_id"]),
                            query=str(item["query"]),
                            runquantity=int(item["runquantity"]),
                            executiontime=int(item["executiontime"]),
                        )
                    )
                except (ValueError, TypeError) as e:
                    return {
                        "error": f"Неверные данные запроса на индексе {i}",
                        "details": f"Ошибка преобразования данных: {e}",
                    }

            validation_result = self._validate_parsed_data(parsed_ddl, parsed_queries)
            if "error" in validation_result:
                return validation_result

            return {"url": url_data, "ddl": parsed_ddl, "queries": parsed_queries}

        except Exception as e:
            self.logger.error(f"Ошибка парсинга полезной нагрузки: {e}")
            return {"error": "Ошибка парсинга полезной нагрузки", "details": str(e)}

    def _validate_parsed_data(
        self, ddl: List[DDLStatement], queries: List[Query]
    ) -> Dict[str, Any]:
        """
        Валидация распарсенных данных.

        :param ddl: Список DDL утверждений
        :param queries: Список запросов
        :return: Результат валидации
        """
        errors = []
        warnings = []

        if not ddl:
            errors.append("Список DDL не может быть пустым")
        else:
            for i, statement in enumerate(ddl):
                if not statement.statement.strip():
                    errors.append(f"DDL утверждение {i} не может быть пустым")

        if not queries:
            errors.append("Список запросов не может быть пустым")
        else:
            query_ids = set()
            for i, query in enumerate(queries):
                if not query.query_id.strip():
                    errors.append(f"Запрос {i} должен иметь непустой query_id")
                elif query.query_id in query_ids:
                    errors.append(f"Найден дубликат query_id: {query.query_id}")
                else:
                    query_ids.add(query.query_id)

                if not query.query.strip():
                    errors.append(f"Запрос {query.query_id} не может быть пустым")

                if query.runquantity < 0:
                    warnings.append(
                        f"Запрос {query.query_id} имеет отрицательное значение runquantity"
                    )

                if query.executiontime < 0:
                    warnings.append(
                        f"Запрос {query.query_id} имеет отрицательное значение executiontime"
                    )

        if errors:
            return {
                "error": "Ошибка валидации",
                "details": errors,
                "warnings": warnings,
            }

        if warnings:
            self.logger.warning(f"Предупреждения валидации: {warnings}")

        return {"status": "valid"}
