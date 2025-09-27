from src.core.prompts.manager import prompt_manager


def get_base_prompt_template() -> str:
    """
    Получить базовый шаблон промпта для анализа схемы.

    :return: Шаблон промпта
    """
    return prompt_manager.load_template("trino_schema_analysis")


BASE_PROMPT_TEMPLATE = get_base_prompt_template()
