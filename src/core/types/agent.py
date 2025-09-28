from typing import Any, Dict, List, TypedDict

from src.core.models.base import DDLStatement, Query


class AgentState(TypedDict):
    """Базовое состояние агента."""

    ddl: List[DDLStatement]
    queries: List[Query]
    url: str
    prompt: str
    response: str
    result: Dict[str, Any]
    chat_history: List[Dict[str, str]]
    schema_info: str
    performance_analysis: str
    data_lineage: str
    schema_diff: str
