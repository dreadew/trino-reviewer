import hashlib
import json
from typing import Any, Dict

import grpc

from src.core.abstractions.review_service import BaseReviewService
from src.core.logging import get_logger
from src.generated.schema_review_pb2 import (
    DDLResult,
    MigrationResult,
    QueryResult,
    ReviewSchemaResponse,
)
from src.generated.schema_review_pb2_grpc import SchemaReviewServiceServicer
from src.infra.cache.valkey_cache import ValkeyCache


class SchemaReviewGRPCService(SchemaReviewServiceServicer):
    """gRPC сервис для анализа схемы БД."""

    def __init__(self, review_service: BaseReviewService):
        """
        Инициализация gRPC сервиса.

        :param review_service: Сервис для проведения ревью схемы
        """
        self.review_service = review_service
        self.logger = get_logger(__name__)
        self.cache = ValkeyCache()

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

            # cache_key = self._generate_cache_key(payload)

            # cached_result = self.cache.get_sync(cache_key)
            # if cached_result is not None:
            #    self.logger.info(
            #        "Результат найден в кэше, возвращаем кэшированный результат"
            #    )
            #    return self._convert_result_to_grpc_response(cached_result)

            result = self.review_service.review(payload)

            # self.cache.set_sync(cache_key, result, ttl=3600)

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

    def _generate_cache_key(self, payload: Dict[str, Any]) -> str:
        """
        Генерация ключа кэша на основе payload запроса.

        :param payload: Payload запроса
        :return: Ключ кэша
        """
        cache_payload = {
            "url": payload["url"],
            "ddl": payload["ddl"],
            "queries": payload["queries"],
        }
        payload_str = json.dumps(cache_payload, sort_keys=True)
        cache_key = hashlib.sha256(payload_str.encode()).hexdigest()
        return f"schema_review:{cache_key}"

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
