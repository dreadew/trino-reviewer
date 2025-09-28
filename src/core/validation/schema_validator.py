from typing import Any, Dict, List

from src.core.logging import get_logger
from src.core.models.base import DDLStatement, Query
from src.core.models.validation import ValidationResult


class SchemaDataValidator:
    """Валидатор данных для схемы."""

    def __init__(self):
        self.logger = get_logger(__name__)

    def validate_ddl_statements(self, ddl: List[DDLStatement]) -> ValidationResult:
        """
        Валидировать DDL утверждения.
        :param ddl: Список DDLStatement
        :return: ValidationResult
        """
        errors = []
        warnings = []

        if not ddl:
            errors.append("DDL statements list cannot be empty")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        for i, statement in enumerate(ddl):
            if not isinstance(statement, DDLStatement):
                errors.append(f"DDL item {i} must be DDLStatement instance")
                continue

            if not statement.statement or not statement.statement.strip():
                errors.append(f"DDL statement {i} cannot be empty")
                continue

            if not self._is_valid_sql_statement(statement.statement):
                warnings.append(f"DDL statement {i} may have syntax issues")

        return ValidationResult(
            is_valid=len(errors) == 0, errors=errors, warnings=warnings
        )

    def validate_queries(self, queries: List[Query]) -> ValidationResult:
        """
        Валидировать запросы.
        :param queries: Список Query
        :return: ValidationResult
        """
        errors = []
        warnings = []

        if not queries:
            errors.append("Queries list cannot be empty")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        query_ids = set()
        for i, query in enumerate(queries):
            if not isinstance(query, Query):
                errors.append(f"Query item {i} must be Query instance")
                continue

            if query.query_id in query_ids:
                errors.append(f"Duplicate query_id: {query.query_id}")
            query_ids.add(query.query_id)

            if not query.query_id or not query.query_id.strip():
                errors.append(f"Query {i} must have non-empty query_id")

            if not query.query or not query.query.strip():
                errors.append(f"Query {i} must have non-empty query")

            if query.runquantity < 0:
                errors.append(f"Query {query.query_id} runquantity cannot be negative")

            if query.executiontime < 0:
                errors.append(
                    f"Query {query.query_id} executiontime cannot be negative"
                )

        return ValidationResult(
            is_valid=len(errors) == 0, errors=errors, warnings=warnings
        )

    def validate_workflow_state(self, state: Dict[str, Any]) -> ValidationResult:
        """
        Валидировать состояние workflow.
        :param state: Словарь состояния
        :return: ValidationResult
        """
        errors = []
        required_keys = ["ddl", "queries"]

        for key in required_keys:
            if key not in state:
                errors.append(f"Missing required key in state: {key}")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    def _is_valid_sql_statement(self, statement: str) -> bool:
        """
        Базовая проверка SQL утверждения.
        :param statement: SQL утверждение
        :return: bool
        """
        statement = statement.strip().upper()
        valid_starts = [
            "CREATE",
            "ALTER",
            "DROP",
            "INSERT",
            "UPDATE",
            "SELECT",
            "DELETE",
        ]
        return any(statement.startswith(start) for start in valid_starts)
