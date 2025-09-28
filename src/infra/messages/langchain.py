from src.core.abstractions.chat_message import BaseChatMessage


class LangChainMessage(BaseChatMessage):
    """Сообщение для LangChain модели."""

    def __init__(self, content: str, role: str = "human"):
        """
        Инициализация сообщения.

        :param content: Содержимое сообщения
        :param role: Роль сообщения (human, system, assistant)
        """
        super().__init__(content)
        self.role = role
