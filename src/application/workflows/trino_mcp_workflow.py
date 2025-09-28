from typing import Dict, Any, List
from src.core.logging import get_logger
from src.application.tools.trino_mcp_tool import TrinoMCPClient
from src.core.config import config

logger = get_logger(__name__)


class TrinoMCPWorkflow:
    """Workflow для расширенной работы с Trino через MCP сервер."""

    def __init__(self, connection_url: str = None):
        """
        Инициализация workflow.

        :param connection_url: URL подключения к Trino
        """
        self.connection_url = connection_url
        self.mcp_server_url = config.TRINO_MCP_SERVER_URL

    async def analyze_existing_schema(
        self, catalog: str, schema: str
    ) -> Dict[str, Any]:
        """
        Полный анализ существующей схемы.

        :param catalog: Каталог
        :param schema: Схема
        :return: Результат анализа
        """
        results = {
            "connection_status": None,
            "catalogs": None,
            "schemas": None,
            "tables": [],
            "documentation": None,
        }

        try:
            async with TrinoMCPClient(
                self.mcp_server_url, self.connection_url
            ) as client:
                logger.info("Проверка подключения к Trino...")
                results["connection_status"] = await client.get_connection_status()

                logger.info("Получение списка каталогов...")
                results["catalogs"] = await client.list_catalogs()

                logger.info(f"Получение схем для каталога {catalog}...")
                results["schemas"] = await client.list_schemas(catalog)

                logger.info(f"Анализ таблиц в схеме {catalog}.{schema}...")

                logger.info("Генерация документации схемы...")
                results["documentation"] = await client.generate_schema_documentation(
                    catalog=catalog, schema=schema
                )

        except Exception as e:
            logger.error(f"Ошибка анализа схемы: {e}")
            results["error"] = str(e)

        return results

    async def validate_and_execute_ddl(
        self, ddl_list: List[str], catalog: str, schema: str, execute: bool = False
    ) -> Dict[str, Any]:
        """
        Валидация и опциональное выполнение DDL.

        :param ddl_list: Список DDL выражений
        :param catalog: Каталог
        :param schema: Схема
        :param execute: Выполнить после валидации
        :return: Результат операции
        """
        results = {"validation": None, "execution": None, "success": False}

        try:
            async with TrinoMCPClient(
                self.mcp_server_url, self.connection_url
            ) as client:
                logger.info("Валидация DDL выражений...")
                results["validation"] = await client.validate_ddl_statements(ddl_list)

                if execute:
                    logger.info("Выполнение DDL выражений...")
                    results["execution"] = await client.execute_ddl_statements(
                        ddl_list=ddl_list,
                        catalog=catalog,
                        schema=schema,
                        validate_first=True,
                    )

                results["success"] = True

        except Exception as e:
            logger.error(f"Ошибка обработки DDL: {e}")
            results["error"] = str(e)

        return results

    async def comprehensive_schema_analysis(
        self, catalog: str, schema: str, proposed_ddl: List[str] = None
    ) -> Dict[str, Any]:
        """
        Комплексный анализ схемы с предложенными изменениями.

        :param catalog: Каталог
        :param schema: Схема
        :param proposed_ddl: Предлагаемые DDL изменения
        :return: Полный отчет
        """
        report = {
            "existing_schema": None,
            "ddl_analysis": None,
            "recommendations": [],
            "timestamp": None,
        }

        try:
            logger.info("Анализ существующей схемы...")
            report["existing_schema"] = await self.analyze_existing_schema(
                catalog, schema
            )

            if proposed_ddl:
                logger.info("Анализ предлагаемых DDL...")
                report["ddl_analysis"] = await self.validate_and_execute_ddl(
                    ddl_list=proposed_ddl,
                    catalog=catalog,
                    schema=schema,
                    execute=False,
                )

            report["recommendations"] = await self._generate_recommendations(report)

            import datetime

            report["timestamp"] = datetime.datetime.now().isoformat()

        except Exception as e:
            logger.error(f"Ошибка комплексного анализа: {e}")
            report["error"] = str(e)

        return report

    async def _generate_recommendations(
        self, analysis_report: Dict[str, Any]
    ) -> List[str]:
        """
        Генерация рекомендаций на основе анализа.

        :param analysis_report: Отчет анализа
        :return: Список рекомендаций
        """
        recommendations = []

        if analysis_report.get("existing_schema", {}).get("connection_status"):
            recommendations.append("Подключение к Trino установлено успешно")
        else:
            recommendations.append("Проблемы с подключением к Trino")

        ddl_analysis = analysis_report.get("ddl_analysis", {})
        if ddl_analysis and ddl_analysis.get("success"):
            recommendations.append("Предлагаемые DDL прошли валидацию")
        elif ddl_analysis:
            recommendations.append("Найдены проблемы в DDL - требуется исправление")

        recommendations.extend(
            [
                "Рекомендуется регулярно обновлять статистику таблиц",
                "Используйте EXPLAIN для анализа производительности запросов",
                "Поддерживайте документацию схемы в актуальном состоянии",
            ]
        )

        return recommendations
