from abc import ABC


class BaseChatMessage(ABC):
    """Базовый класс для сообщений чата."""

    def __init__(self, content: str):
        """
        Инициализация сообщения.

        :param content: Содержимое сообщения
        """
        self.content = content
