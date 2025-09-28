from concurrent import futures

import grpc

from src.api.grpc.services.schema_review import SchemaReviewGRPCService
from src.core.abstractions.review_service import BaseReviewService
from src.core.config import config
from src.core.logging import get_logger
from src.generated.schema_review_pb2_grpc import (
    add_SchemaReviewServiceServicer_to_server,
)


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
