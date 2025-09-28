import re
from typing import List

from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field

from src.core.logging import get_logger

logger = get_logger(__name__)


class DataLineageInput(BaseModel):
    queries: List[str] = Field(description="SQL запросы для анализа зависимостей")


class DataLineageTool(BaseTool):
    """
    Tool для построения графа зависимостей таблиц.
    - Строит граф зависимостей по SQL
    - Выявляет критические пути
    - Анализирует влияние изменений схемы
    """

    name: str = "data_lineage"
    description: str = (
        "Анализирует SQL запросы, строит граф зависимостей таблиц и выявляет критические пути данных."
    )
    args_schema: type = DataLineageInput

    def _extract_tables(self, query: str) -> List[str]:

        tables = re.findall(r"FROM\s+([\w\.]+)", query, re.IGNORECASE)
        tables += re.findall(r"JOIN\s+([\w\.]+)", query, re.IGNORECASE)
        return list(set(tables))

    def _run(self, queries: List[str]) -> str:
        try:
            logger.info(f"Анализ data lineage для {len(queries)} запросов")
            graph = {}
            for q in queries:
                tables = self._extract_tables(q)
                for t in tables:
                    if t not in graph:
                        graph[t] = set()

                    join_tables = re.findall(r"JOIN\s+([\w\.]+)", q, re.IGNORECASE)
                    for jt in join_tables:
                        graph[t].add(jt)

            result = ["=== ГРАФ ЗАВИСИМОСТЕЙ ТАБЛИЦ ==="]
            for table, deps in graph.items():
                result.append(
                    f"{table} -> {', '.join(deps) if deps else 'нет зависимостей'}"
                )

            critical = sorted(graph.items(), key=lambda x: len(x[1]), reverse=True)
            if critical:
                result.append("\n-- Критические таблицы:")
                for t, deps in critical[:3]:
                    result.append(f"{t}: {len(deps)} зависимостей")
            return "\n".join(result)
        except Exception as e:
            logger.error(f"Ошибка анализа data lineage: {e}")
            return f"Ошибка анализа data lineage: {str(e)}"

    async def _arun(self, queries: List[str]) -> str:
        return self._run(queries)


def create_data_lineage_tool() -> DataLineageTool:
    return DataLineageTool()
