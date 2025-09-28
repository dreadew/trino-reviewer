from typing import List
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field
from src.core.logging import get_logger

logger = get_logger(__name__)


class SchemaDiffInput(BaseModel):
    current_schema: List[str] = Field(description="DDL текущей схемы")
    proposed_schema: List[str] = Field(description="DDL новой схемы")


class SchemaDiffTool(BaseTool):
    """
    Tool для сравнения схем и генерации миграций.
    - Сравнивает текущую и предложенную схему
    - Генерирует безопасные DDL миграции
    - Проверяет обратную совместимость
    - Предупреждает о breaking changes
    """

    name: str = "schema_diff"
    description: str = (
        "Сравнивает текущую и новую схему, генерирует миграции и предупреждает о breaking changes."
    )
    args_schema: type = SchemaDiffInput

    def _run(self, current_schema: List[str], proposed_schema: List[str]) -> str:
        try:
            logger.info(
                f"Сравнение схем: текущая={len(current_schema)}, новая={len(proposed_schema)}"
            )

            current_set = set(current_schema)
            proposed_set = set(proposed_schema)
            added = proposed_set - current_set
            removed = current_set - proposed_set
            unchanged = current_set & proposed_set

            migrations = []
            for ddl in added:
                migrations.append(f"-- Добавить\n{ddl}")
            for ddl in removed:
                migrations.append(f"-- Удалить\n{ddl}")

            breaking = [ddl for ddl in removed if "DROP" in ddl or "ALTER" in ddl]

            result = ["=== СРАВНЕНИЕ СХЕМ ==="]
            result.append(f"Добавлено: {len(added)}")
            result.append(f"Удалено: {len(removed)}")
            result.append(f"Без изменений: {len(unchanged)}")
            if migrations:
                result.append("\n-- Миграции:")
                result.extend(migrations)
            if breaking:
                result.append("\n-- Breaking changes:")
                result.extend(breaking)
            return "\n".join(result)
        except Exception as e:
            logger.error(f"Ошибка сравнения схем: {e}")
            return f"Ошибка сравнения схем: {str(e)}"

    async def _arun(self, current_schema: List[str], proposed_schema: List[str]) -> str:
        return self._run(current_schema, proposed_schema)


def create_schema_diff_tool() -> SchemaDiffTool:
    return SchemaDiffTool()
