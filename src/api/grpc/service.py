from concurrent import futures
from typing import Any, Dict

import grpc

from src.core.abstractions.review_service import BaseReviewService
from src.core.config import config
from src.core.logging import get_logger
from src.generated.schema_review_pb2 import (
    DDLResult,
    MigrationResult,
    QueryResult,
    ReviewSchemaResponse,
)
from src.generated.schema_review_pb2_grpc import (
    SchemaReviewServiceServicer,
    add_SchemaReviewServiceServicer_to_server,
)


class SchemaReviewGRPCService(SchemaReviewServiceServicer):
    """gRPC сервис для анализа схемы БД."""

    def __init__(self, review_service: BaseReviewService):
        """
        Инициализация gRPC сервиса.

        :param review_service: Сервис для проведения ревью схемы
        """
        self.review_service = review_service
        self.logger = get_logger(__name__)

    def ReviewSchema(self, request, context):
        """
        Обработка запроса на ревью схемы.

        :param request: gRPC запрос
        :param context: gRPC контекст
        :return: gRPC ответ
        """
        try:
            self.logger.info(f"Получен запрос на ревью схемы для URL: {request.url}")

            payload = self._convert_grpc_request_to_payload(request)

            result = self.review_service.review(payload)

            response = self._convert_result_to_grpc_response(result)

            self.logger.info("Ревью схемы успешно завершено")
            return response

        except Exception as e:
            self.logger.error(f"Ошибка при обработке запроса ревью: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal server error: {str(e)}")

            return ReviewSchemaResponse(
                success=False, message="Internal server error", error=str(e)
            )

    def _convert_grpc_request_to_payload(self, request) -> Dict[str, Any]:
        """
        Преобразование gRPC запроса во внутренний формат payload.

        :param request: gRPC запрос
        :return: Payload для внутреннего использования
        """
        ddl_statements = []
        for ddl_proto in request.ddl:
            ddl_statements.append({"statement": ddl_proto.statement})

        queries = []
        for query_proto in request.queries:
            queries.append(
                {
                    "query_id": query_proto.query_id,
                    "query": query_proto.query,
                    "runquantity": query_proto.runquantity,
                    "executiontime": query_proto.executiontime,
                }
            )

        payload = {"url": request.url, "ddl": ddl_statements, "queries": queries}

        if request.HasField("thread_id"):
            payload["thread_id"] = request.thread_id

        return payload

    def _convert_result_to_grpc_response(
        self, result: Dict[str, Any]
    ) -> ReviewSchemaResponse:
        """
        Преобразование внутреннего результата в gRPC ответ.

        :param result: Результат внутреннего сервиса
        :return: gRPC ответ
        """
        if "error" in result:
            return ReviewSchemaResponse(
                success=False,
                message="Validation or processing error",
                error=result["error"],
                warnings=result.get("warnings", []),
            )

        response = ReviewSchemaResponse(
            success=True, message="Schema review completed successfully"
        )

        if "ddl" in result and result["ddl"]:
            for ddl_item in result["ddl"]:
                if isinstance(ddl_item, dict) and "statement" in ddl_item:
                    response.ddl.append(DDLResult(statement=ddl_item["statement"]))

        if "migrations" in result and result["migrations"]:
            for migration_item in result["migrations"]:
                if isinstance(migration_item, dict) and "statement" in migration_item:
                    response.migrations.append(
                        MigrationResult(statement=migration_item["statement"])
                    )

        if "queries" in result and result["queries"]:
            for query_item in result["queries"]:
                if (
                    isinstance(query_item, dict)
                    and "query_id" in query_item
                    and "query" in query_item
                ):
                    response.queries.append(
                        QueryResult(
                            query_id=query_item["query_id"], query=query_item["query"]
                        )
                    )
        if "warnings" in result:
            response.warnings.extend(result["warnings"])

        return response


class GRPCServer:
    """gRPC сервер для анализа схемы БД."""

    def __init__(self, review_service: BaseReviewService, port: int = None):
        """
        Инициализация gRPC сервера.

        :param review_service: Сервис для проведения ревью
        :param port: Порт для запуска сервера (по умолчанию из конфигурации)
        """
        self.review_service = review_service
        self.port = port or config.GRPC_PORT
        self.server = None
        self.logger = get_logger(__name__)

    def start(self):
        """
        Запуск gRPC сервера.
        """
        max_workers = config.GRPC_MAX_WORKERS
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))

        schema_review_service = SchemaReviewGRPCService(self.review_service)
        add_SchemaReviewServiceServicer_to_server(schema_review_service, self.server)

        listen_addr = f"[::]:{self.port}"
        self.server.add_insecure_port(listen_addr)

        self.server.start()
        self.logger.info(
            f"gRPC сервер запущен на порту {self.port} с {max_workers} воркерами"
        )

        try:
            self.server.wait_for_termination()
        except KeyboardInterrupt:
            self.logger.info("Получен сигнал прерывания, останавливаем сервер...")
            self.stop()

    def stop(self):
        """
        Остановка gRPC сервера.
        """
        if self.server:
            grace_period = config.GRPC_GRACE_PERIOD
            self.server.stop(grace=grace_period)
            self.logger.info(f"gRPC сервер остановлен с grace period {grace_period}s")
