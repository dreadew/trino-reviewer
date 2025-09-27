from src.core.prompts.manager import prompt_manager


def get_system_reviewer_prompt() -> str:
    """
    Получить системный промпт для агента-ревьюера.

    :return: Системный промпт
    """
    return prompt_manager.load_template("system_reviewer")


SYSTEM_REVIEWER_PROMPT = get_system_reviewer_prompt()
