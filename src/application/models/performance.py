from dataclasses import dataclass


@dataclass
class QueryMetrics:
    """Метрики производительности запроса."""

    query_id: str
    query: str
    execution_time: float
    run_quantity: int
    total_time: float
    priority_score: float


@dataclass
class PerformanceRecommendation:
    """Рекомендация по оптимизации."""

    query_id: str
    issue_type: str
    description: str
    recommendation: str
    impact: str
