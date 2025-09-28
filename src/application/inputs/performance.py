from typing import Any, Dict, List

from pydantic import BaseModel, Field


class PerformanceAnalysisInput(BaseModel):
    """Входные параметры для анализа производительности."""

    queries: List[Dict[str, Any]] = Field(
        description="SQL запросы с метриками производительности"
    )
