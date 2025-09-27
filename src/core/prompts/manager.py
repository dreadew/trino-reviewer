from pathlib import Path

from src.core.logging import get_logger


class PromptManager:
    """Менеджер для загрузки и управления промптами."""

    def __init__(self, templates_dir: str = None):
        """
        Инициализация менеджера промптов.

        :param templates_dir: Путь к директории с шаблонами
        """
        self.logger = get_logger(__name__)
        self.templates_dir = Path(templates_dir or self._get_default_templates_dir())
        self._cache = {}

    def _get_default_templates_dir(self) -> str:
        """
        Получить путь к директории шаблонов по умолчанию.

        :return: Путь к директории
        """
        current_dir = Path(__file__).parent
        return current_dir / "templates"

    def load_template(self, template_name: str) -> str:
        """
        Загрузить шаблон промпта.

        :param template_name: Имя шаблона
        :return: Содержимое шаблона
        """
        if template_name in self._cache:
            return self._cache[template_name]

        template_path = self.templates_dir / f"{template_name}.txt"

        if not template_path.exists():
            raise FileNotFoundError(
                f"Шаблон {template_name} не найден: {template_path}"
            )

        try:
            with open(template_path, "r", encoding="utf-8") as f:
                content = f.read().strip()

            self._cache[template_name] = content
            self.logger.info(f"Загружен шаблон: {template_name}")
            return content

        except Exception as e:
            self.logger.error(f"Ошибка загрузки шаблона {template_name}: {e}")
            raise

    def format_template(self, template_name: str, **kwargs) -> str:
        """
        Загрузить и отформатировать шаблон с параметрами.

        :param template_name: Имя шаблона
        :param kwargs: Параметры для форматирования
        :return: Отформатированный промпт
        """
        template = self.load_template(template_name)
        try:
            return template.format(**kwargs)
        except KeyError as e:
            self.logger.error(f"Отсутствует параметр {e} для шаблона {template_name}")
            raise

    def reload_template(self, template_name: str) -> str:
        """
        Перезагрузить шаблон из файла.

        :param template_name: Имя шаблона
        :return: Содержимое шаблона
        """
        if template_name in self._cache:
            del self._cache[template_name]
        return self.load_template(template_name)

    def clear_cache(self) -> None:
        """
        Очистить кэш шаблонов.
        """
        self._cache.clear()
        self.logger.info("Кэш шаблонов очищен")


prompt_manager = PromptManager()
