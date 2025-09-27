import signal
import sys
from typing import Optional

from src.api.grpc.grpc_service import GRPCServer
from src.core.config import config
from src.core.factories.service_factory import service_factory
from src.core.logging import get_logger


class Application:
    """Главное приложение."""

    def __init__(self):
        """
        Инициализация приложения.
        """
        self.logger = get_logger(__name__)
        self.grpc_server: Optional[GRPCServer] = None

    def initialize(self) -> bool:
        """
        Инициализация всех компонентов приложения.

        :return: True если инициализация прошла успешно
        """
        try:
            self.logger.info(f"Инициализация приложения {config.APP_NAME}")

            if not service_factory.validate_configuration():
                self.logger.error("Ошибка валидации конфигурации")
                return False

            review_service = service_factory.create_review_service()
            self.logger.info("Сервис ревью создан успешно")

            self.grpc_server = GRPCServer(
                review_service=review_service, port=config.GRPC_PORT
            )
            self.logger.info(f"gRPC сервер создан на порту {config.GRPC_PORT}")

            return True

        except Exception as e:
            self.logger.error(f"Ошибка инициализации приложения: {e}")
            return False

    def start(self) -> None:
        """
        Запуск приложения.
        """
        if not self.initialize():
            self.logger.error("Не удалось инициализировать приложение")
            sys.exit(1)

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self.logger.info(f"Запуск {config.APP_NAME}...")
            self.grpc_server.start()
        except Exception as e:
            self.logger.error(f"Ошибка запуска приложения: {e}")
            sys.exit(1)

    def stop(self) -> None:
        """
        Остановка приложения.
        """
        self.logger.info("Остановка приложения...")
        if self.grpc_server:
            self.grpc_server.stop()
        self.logger.info("Приложение остановлено")

    def _signal_handler(self, signum, frame) -> None:
        """
        Обработчик системных сигналов.

        :param signum: Номер сигнала
        :param frame: Кадр выполнения
        """
        self.logger.info(f"Получен сигнал {signum}, остановка приложения...")
        self.stop()
        sys.exit(0)
