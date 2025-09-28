import re
from typing import Any, Dict, List

from langchain_core.tools import BaseTool

from src.application.inputs.performance import PerformanceAnalysisInput
from src.application.models.performance import PerformanceRecommendation, QueryMetrics
from src.core.logging import get_logger

logger = get_logger(__name__)


class PerformanceAnalysisTool(BaseTool):
    """
    LangGraph tool для анализа производительности SQL запросов.

    Анализирует execution_time и run_quantity для выявления:
    - Самых медленных запросов
    - Наиболее часто выполняемых запросов
    - Запросов с наибольшим суммарным временем выполнения
    - Приоритет оптимизации на основе метрик
    """

    name: str = "performance_analyzer"
    description: str = (
        "Анализирует метрики производительности SQL запросов. "
        "Выявляет узкие места, рассчитывает приоритеты оптимизации, "
        "предлагает рекомендации по улучшению производительности."
    )
    args_schema: type = PerformanceAnalysisInput

    def _calculate_priority_score(
        self, execution_time: float, run_quantity: int
    ) -> float:
        """
        Рассчитать приоритет оптимизации запроса.

        Формула: (execution_time * run_quantity^0.7) / 1000
        - Учитывает как время выполнения, так и частоту
        - Степень 0.7 для run_quantity уменьшает влияние очень частых запросов

        :param execution_time: Время выполнения в мс
        :param run_quantity: Количество выполнений
        :return: Приоритет оптимизации
        """
        if execution_time <= 0 or run_quantity <= 0:
            return 0.0

        return (execution_time * (run_quantity**0.7)) / 1000

    def _analyze_query_patterns(self, query: str) -> List[str]:
        """
        Анализировать паттерны в SQL запросе для выявления проблем.

        :param query: SQL запрос
        :return: Список выявленных проблем
        """
        issues = []
        query_upper = query.upper()

        if (
            "SELECT" in query_upper
            and "WHERE" not in query_upper
            and "LIMIT" not in query_upper
        ):
            issues.append("full_table_scan")

        join_count = len(
            re.findall(r"\b(?:INNER|LEFT|RIGHT|OUTER)?\s*JOIN\b", query_upper)
        )
        if join_count >= 3:
            issues.append("complex_joins")

        if re.search(r"WHERE.*\b(?:UPPER|LOWER|SUBSTRING|CONCAT)\s*\(", query_upper):
            issues.append("functions_in_where")

        if re.search(r"SELECT.*\(\s*SELECT", query_upper):
            issues.append("subquery_in_select")

        if "DISTINCT" in query_upper and "ORDER BY" not in query_upper:
            issues.append("unordered_distinct")

        if (
            re.search(r"\b(?:COUNT|SUM|AVG|MAX|MIN)\s*\(.*\)", query_upper)
            and "GROUP BY" not in query_upper
        ):
            issues.append("aggregation_without_grouping")

        return issues

    def _generate_recommendations(
        self, metrics: QueryMetrics, issues: List[str]
    ) -> List[PerformanceRecommendation]:
        """
        Генерировать рекомендации на основе метрик и выявленных проблем.

        :param metrics: Метрики запроса
        :param issues: Выявленные проблемы
        :return: Список рекомендаций
        """
        recommendations = []

        if metrics.execution_time > 5000:
            recommendations.append(
                PerformanceRecommendation(
                    query_id=metrics.query_id,
                    issue_type="slow_execution",
                    description=f"Запрос выполняется {metrics.execution_time}мс, что очень медленно",
                    recommendation="Создать индексы для столбцов в WHERE и JOIN условиях без изменения результата запроса",
                    impact="high",
                )
            )
        elif metrics.execution_time > 1000:
            recommendations.append(
                PerformanceRecommendation(
                    query_id=metrics.query_id,
                    issue_type="moderate_execution",
                    description=f"Запрос выполняется {metrics.execution_time}мс",
                    recommendation="Добавить индексы на столбцы фильтрации и соединений для ускорения без изменения результата",
                    impact="medium",
                )
            )

        if metrics.run_quantity > 10000:
            recommendations.append(
                PerformanceRecommendation(
                    query_id=metrics.query_id,
                    issue_type="high_frequency",
                    description=f"Запрос выполняется {metrics.run_quantity} раз",
                    recommendation="Создать материализованное представление с идентичной структурой результата",
                    impact="high",
                )
            )

        for issue in issues:
            if issue == "full_table_scan":
                recommendations.append(
                    PerformanceRecommendation(
                        query_id=metrics.query_id,
                        issue_type="full_table_scan",
                        description="Запрос выполняет полное сканирование таблицы",
                        recommendation="Создать индексы на столбцы для фильтрации без изменения логики запроса",
                        impact="high",
                    )
                )
            elif issue == "complex_joins":
                recommendations.append(
                    PerformanceRecommendation(
                        query_id=metrics.query_id,
                        issue_type="complex_joins",
                        description="Запрос содержит множественные JOIN операции",
                        recommendation="Создать составные индексы для JOIN столбцов или партицировать таблицы",
                        impact="high",
                    )
                )
            elif issue == "functions_in_where":
                recommendations.append(
                    PerformanceRecommendation(
                        query_id=metrics.query_id,
                        issue_type="functions_in_where",
                        description="Использование функций в WHERE предотвращает использование индексов",
                        recommendation="Создать функциональные индексы для выражений в WHERE",
                        impact="medium",
                    )
                )
            elif issue == "subquery_in_select":
                recommendations.append(
                    PerformanceRecommendation(
                        query_id=metrics.query_id,
                        issue_type="subquery_in_select",
                        description="Подзапросы в SELECT могут выполняться для каждой строки",
                        recommendation="Создать материализованное представление или индексы для подзапросов",
                        impact="medium",
                    )
                )

        return recommendations

    def _run(self, queries: List[Dict[str, Any]]) -> str:
        """
        Выполнить анализ производительности запросов.

        :param queries: Список запросов с метриками
        :return: Результат анализа в текстовом формате
        """
        try:
            logger.info(f"Анализ производительности для {len(queries)} запросов")

            metrics_list = []
            for q in queries:
                execution_time = float(q.get("executiontime", 0))
                run_quantity = int(q.get("runquantity", 1))

                metrics = QueryMetrics(
                    query_id=q.get("query_id", "unknown"),
                    query=q.get("query", ""),
                    execution_time=execution_time,
                    run_quantity=run_quantity,
                    total_time=execution_time * run_quantity,
                    priority_score=self._calculate_priority_score(
                        execution_time, run_quantity
                    ),
                )
                metrics_list.append(metrics)

            metrics_list.sort(key=lambda x: x.priority_score, reverse=True)

            all_recommendations = []
            for metrics in metrics_list:
                issues = self._analyze_query_patterns(metrics.query)
                recommendations = self._generate_recommendations(metrics, issues)
                all_recommendations.extend(recommendations)

            result_lines = []
            result_lines.append("=== АНАЛИЗ ПРОИЗВОДИТЕЛЬНОСТИ SQL ЗАПРОСОВ ===\n")
            result_lines.append(
                "⚠️ ВАЖНО: Оптимизируем только СХЕМУ БД, не изменяя SQL запросы!"
            )
            result_lines.append(
                "Результаты запросов (столбцы, записи) должны остаться идентичными.\n"
            )

            result_lines.append("ТОП-5 ЗАПРОСОВ ДЛЯ ОПТИМИЗАЦИИ (по приоритету):")
            for i, metrics in enumerate(metrics_list[:5], 1):
                result_lines.append(
                    f"{i}. Query ID: {metrics.query_id}\n"
                    f"   Время выполнения: {metrics.execution_time}мс\n"
                    f"   Количество выполнений: {metrics.run_quantity}\n"
                    f"   Суммарное время: {metrics.total_time}мс\n"
                    f"   Приоритет оптимизации: {metrics.priority_score:.2f}\n"
                )

            total_queries = len(metrics_list)
            slow_queries = len([m for m in metrics_list if m.execution_time > 1000])
            frequent_queries = len([m for m in metrics_list if m.run_quantity > 1000])

            result_lines.append(f"\nОБЩАЯ СТАТИСТИКА:")
            result_lines.append(f"- Всего запросов: {total_queries}")
            result_lines.append(f"- Медленных запросов (>1с): {slow_queries}")
            result_lines.append(f"- Частых запросов (>1000 раз): {frequent_queries}")

            if all_recommendations:
                result_lines.append(
                    f"\nРЕКОМЕНДАЦИИ ПО ОПТИМИЗАЦИИ СХЕМЫ (БЕЗ ИЗМЕНЕНИЯ ЗАПРОСОВ):"
                )
                high_impact = [r for r in all_recommendations if r.impact == "high"]
                medium_impact = [r for r in all_recommendations if r.impact == "medium"]

                if high_impact:
                    result_lines.append(f"\nВЫСОКИЙ ПРИОРИТЕТ ({len(high_impact)}):")
                    for rec in high_impact[:10]:
                        result_lines.append(f"- {rec.query_id}: {rec.description}")
                        result_lines.append(
                            f"  Оптимизация схемы: {rec.recommendation}"
                        )

                if medium_impact:
                    result_lines.append(f"\nСРЕДНИЙ ПРИОРИТЕТ ({len(medium_impact)}):")
                    for rec in medium_impact[:5]:
                        result_lines.append(f"- {rec.query_id}: {rec.description}")
                        result_lines.append(
                            f"  Оптимизация схемы: {rec.recommendation}"
                        )
            else:
                result_lines.append(f"\nВсе запросы работают оптимально!")

            result_lines.append(
                f"\n📋 ПРИНЦИП: Все рекомендации направлены на изменение схемы БД"
            )
            result_lines.append(
                f"(индексы, партиции, мат.представления) без изменения SQL запросов."
            )

            return "\n".join(result_lines)

        except Exception as e:
            error_msg = f"Ошибка при анализе производительности: {str(e)}"
            logger.error(error_msg)
            return error_msg

    async def _arun(self, queries: List[Dict[str, Any]]) -> str:
        """Асинхронная версия."""
        return self._run(queries)


def create_performance_analysis_tool() -> PerformanceAnalysisTool:
    """
    Фабричная функция для создания Performance Analysis tool.

    :return: Настроенный PerformanceAnalysisTool
    """
    return PerformanceAnalysisTool()
