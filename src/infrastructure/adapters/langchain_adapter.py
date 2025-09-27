from typing import List

from langchain.schema import AIMessage, HumanMessage, SystemMessage

from src.core.abstractions.chat_model import BaseChatMessage, ChatModel
from src.core.logging import get_logger


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


class LangChainChatModelAdapter(ChatModel):
    """Адаптер для LangChain моделей чата."""

    def __init__(self, langchain_model):
        """
        Инициализация адаптера.

        :param langchain_model: Модель LangChain
        """
        self.langchain_model = langchain_model
        self.logger = get_logger(__name__)

    def invoke(self, messages: List[BaseChatMessage]) -> str:
        """
        Вызвать модель с набором сообщений.

        :param messages: Список сообщений
        :return: Ответ модели
        """
        try:
            langchain_messages = []
            for msg in messages:
                if isinstance(msg, LangChainMessage):
                    if msg.role == "system":
                        langchain_messages.append(SystemMessage(content=msg.content))
                    elif msg.role == "assistant":
                        langchain_messages.append(AIMessage(content=msg.content))
                    else:
                        langchain_messages.append(HumanMessage(content=msg.content))
                else:
                    langchain_messages.append(HumanMessage(content=msg.content))

            response = self.langchain_model.invoke(langchain_messages)

            if hasattr(response, "content"):
                return response.content
            else:
                return str(response)

        except Exception as e:
            self.logger.error(f"Ошибка при вызове LangChain модели: {e}")
            raise

    def get_model_name(self) -> str:
        """
        Получить название модели.

        :return: Название модели
        """
        if hasattr(self.langchain_model, "model_name"):
            return self.langchain_model.model_name
        elif hasattr(self.langchain_model, "model"):
            return self.langchain_model.model
        else:
            return self.langchain_model.__class__.__name__
