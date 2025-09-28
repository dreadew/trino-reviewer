from pydantic import BaseModel, Field
from typing import List, Dict, Any


class PerformanceAnalysisInput(BaseModel):
    """Входные параметры для анализа производительности."""

    queries: List[Dict[str, Any]] = Field(
        description="SQL запросы с метриками производительности"
    )
