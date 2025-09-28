from src.application.agents.schema_reviewer import SchemaReviewerAgent
from src.application.handlers.llm_message_handler import LLMMessageHandler
from src.application.services.llm import LLMService
from src.application.services.review import SchemaReviewService
from src.application.workflows.analyze_schema import AnalyzeSchemaWorkflow
from src.core.abstractions.agent import BaseAgent
from src.core.abstractions.chat_model import ChatModel
from src.core.abstractions.llm import BaseLLMService
from src.core.abstractions.message_handler import BaseMessageHandler
from src.core.abstractions.review_service import BaseReviewService
from src.core.abstractions.workflow import BaseWorkflow
from src.core.config import config
from src.core.logging import get_logger
from src.infrastructure.adapters.langchain_adapter import LangChainChatModelAdapter


class ServiceFactory:
    """Фабрика для создания сервисов приложения."""

    def __init__(self):
        """
        Инициализация фабрики сервисов.
        """
        self.logger = get_logger(__name__)
        self._llm_service = None
        self._message_handler = None
        self._workflow = None
        self._agent = None
        self._review_service = None

    def create_llm_service(self) -> BaseLLMService:
        """
        Создать сервис LLM.

        :return: Сервис LLM
        """
        if self._llm_service is None:
            self.logger.info(f"Создание LLM сервиса для модели: {config.MODEL_TYPE}")

            model = self._create_llm_model()
            self._llm_service = LLMService(model=model)
            self.logger.info(
                f"Создан {config.MODEL_TYPE} сервис с моделью {self._get_model_name()}"
            )

        return self._llm_service

    def _create_llm_model(self):
        """
        Создать модель LLM на основе конфигурации.

        :return: Модель LLM (для LLMService)
        """
        return self._create_raw_langchain_model()

    def _create_chat_model(self) -> ChatModel:
        """
        Создать ChatModel на основе конфигурации.

        :return: ChatModel адаптер
        """
        langchain_model = self._create_raw_langchain_model()
        return LangChainChatModelAdapter(langchain_model)

    def _create_raw_langchain_model(self):
        """
        Создать исходную LangChain модель.

        :return: LangChain модель
        """
        model_type = config.MODEL_TYPE.lower()

        if model_type == "giga":
            return self._create_gigachat_model()
        elif model_type == "openai":
            return self._create_openai_model()
        elif model_type == "gemini":
            return self._create_gemini_model()
        else:
            raise ValueError(f"Неподдерживаемый тип модели: {config.MODEL_TYPE}")

    def _create_gigachat_model(self):
        """
        Создать модель GigaChat.

        :return: Модель GigaChat
        """
        if not config.API_KEY:
            raise ValueError("API_KEY не установлен для GigaChat")

        try:
            from langchain_gigachat import GigaChat

            return GigaChat(
                credentials=config.API_KEY,
                model=config.MODEL_NAME,
                max_tokens=config.MAX_TOKENS,
                temperature=config.TEMPERATURE,
                verify_ssl_certs=False,
            )
        except ImportError:
            raise ValueError(
                "langchain_gigachat не установлен. Установите: pip install langchain-gigachat"
            )

    def _create_openai_model(self):
        """
        Создать модель OpenAI.

        :return: Модель OpenAI
        """
        if not config.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY не установлен для OpenAI")

        try:
            from langchain_openai import ChatOpenAI

            kwargs = {
                "api_key": config.OPENAI_API_KEY,
                "model": config.OPENAI_MODEL_NAME,
                "max_tokens": config.MAX_TOKENS,
                "temperature": config.TEMPERATURE,
            }

            if config.OPENAI_BASE_URL:
                kwargs["base_url"] = config.OPENAI_BASE_URL

            return ChatOpenAI(**kwargs)
        except ImportError:
            raise ValueError(
                "langchain_openai не установлен. Установите: pip install langchain-openai"
            )

    def _create_gemini_model(self):
        """
        Создать модель Google Gemini.

        :return: Модель Gemini
        """
        if not config.GOOGLE_API_KEY:
            raise ValueError("GOOGLE_API_KEY не установлен для Gemini")

        try:
            from langchain_google_genai import ChatGoogleGenerativeAI

            return ChatGoogleGenerativeAI(
                google_api_key=config.GOOGLE_API_KEY,
                model=config.GEMINI_MODEL_NAME,
                max_tokens=config.MAX_TOKENS,
                temperature=config.TEMPERATURE,
            )
        except ImportError:
            raise ValueError(
                "langchain_google_genai не установлен. Установите: pip install langchain-google-genai"
            )

    def _get_model_name(self) -> str:
        """
        Получить имя модели для логирования.

        :return: Имя модели
        """
        model_type = config.MODEL_TYPE.lower()
        if model_type == "giga":
            return config.MODEL_NAME
        elif model_type == "openai":
            return config.OPENAI_MODEL_NAME
        elif model_type == "gemini":
            return config.GEMINI_MODEL_NAME
        return "unknown"

    def create_message_handler(self) -> BaseMessageHandler:
        """
        Создать обработчик сообщений.

        :return: Обработчик сообщений
        """
        if self._message_handler is None:
            self.logger.info("Создание обработчика сообщений")
            llm_service = self.create_llm_service()
            self._message_handler = LLMMessageHandler(llm_service=llm_service)

        return self._message_handler

    def create_workflow(self) -> BaseWorkflow:
        """
        Создать workflow для анализа схемы.

        :return: Workflow
        """
        if self._workflow is None:
            self.logger.info("Создание workflow для анализа схемы")
            message_handler = self.create_message_handler()
            self._workflow = AnalyzeSchemaWorkflow(message_handler=message_handler)

        return self._workflow

    def create_agent(self) -> BaseAgent:
        """
        Создать агента для анализа схемы.

        :return: Агент
        """
        if self._agent is None:
            self.logger.info("Создание агента для анализа схемы")

            chat_model = self._create_chat_model()

            llm_service = self.create_llm_service()
            workflow = self.create_workflow()

            self._agent = SchemaReviewerAgent(
                model=chat_model, llm_service=llm_service, workflow=workflow
            )

            self.logger.info("Создан SchemaReviewerAgent")

        return self._agent

    def create_review_service(self) -> BaseReviewService:
        """
        Создать сервис ревью схемы.

        :return: Сервис ревью
        """
        if self._review_service is None:
            self.logger.info("Создание сервиса ревью схемы")
            agent = self.create_agent()
            self._review_service = SchemaReviewService(agent=agent)

        return self._review_service

    def validate_configuration(self) -> bool:
        """
        Валидация конфигурации приложения.

        :return: True если конфигурация валидна
        """
        errors = []

        model_type = config.MODEL_TYPE.lower()

        if model_type == "giga":
            if not config.API_KEY:
                errors.append("API_KEY не установлен для GigaChat")
        elif model_type == "openai":
            if not config.OPENAI_API_KEY:
                errors.append("OPENAI_API_KEY не установлен для OpenAI")
        elif model_type == "gemini":
            if not config.GOOGLE_API_KEY:
                errors.append("GOOGLE_API_KEY не установлен для Gemini")
        else:
            errors.append(f"Неподдерживаемый тип модели: {config.MODEL_TYPE}")

        if config.GRPC_PORT < 1 or config.GRPC_PORT > 65535:
            errors.append(f"Некорректный GRPC_PORT: {config.GRPC_PORT}")

        if config.MAX_TOKENS < 1:
            errors.append(f"Некорректный MAX_TOKENS: {config.MAX_TOKENS}")

        if errors:
            self.logger.error(f"Ошибки конфигурации: {errors}")
            return False

        self.logger.info("Конфигурация валидна")
        return True

    def create_cache_service(self):
        """
        Создать сервис кэширования Valkey.

        :return: Экземпляр ValkeyCache
        """
        try:
            from src.infrastructure.cache.valkey_cache import ValkeyCache

            cache = ValkeyCache(
                host=(
                    config.VALKEY_HOST
                    if hasattr(config, "VALKEY_HOST")
                    else "localhost"
                ),
                port=config.VALKEY_PORT if hasattr(config, "VALKEY_PORT") else 6379,
                db=config.VALKEY_DB if hasattr(config, "VALKEY_DB") else 0,
                password=(
                    config.VALKEY_PASSWORD
                    if hasattr(config, "VALKEY_PASSWORD")
                    else None
                ),
            )

            self.logger.info("Создан сервис Valkey кэша")
            return cache

        except Exception as e:
            self.logger.error(f"Ошибка при создании Valkey кэша: {e}")
            raise

    def create_prompt_service(self):
        """
        Создать сервис для управления промптами.

        :return: Экземпляр PromptService
        """
        try:
            from src.application.services.prompt_service import PromptService

            cache = self.create_cache_service()
            prompt_service = PromptService(
                cache=cache,
                ttl=(
                    config.PROMPT_CACHE_TTL
                    if hasattr(config, "PROMPT_CACHE_TTL")
                    else 3600
                ),
            )

            self.logger.info("Создан сервис промптов с Valkey кэшированием")
            return prompt_service

        except Exception as e:
            self.logger.error(f"Ошибка при создании сервиса промптов: {e}")
            raise

    def create_prompt_manager(self):
        """
        Создать менеджер промптов (алиас для обратной совместимости).

        Теперь возвращает PromptService для устранения дублирования.

        :return: Экземпляр PromptService
        """
        self.logger.warning(
            "create_prompt_manager устарел, используйте create_prompt_service"
        )
        return self.create_prompt_service()


service_factory = ServiceFactory()
