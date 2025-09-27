import json
import re


def _find_json_objects(text: str) -> list:
    """Найти JSON объекты {} в тексте."""
    candidates = []
    brace_count = 0
    start_pos = -1

    for i, char in enumerate(text):
        if char == "{":
            if brace_count == 0:
                start_pos = i
            brace_count += 1
        elif char == "}":
            brace_count -= 1
            if brace_count == 0 and start_pos != -1:
                candidate = text[start_pos : i + 1]
                candidates.append(candidate)
                start_pos = -1

    return candidates


def _find_json_arrays(text: str) -> list:
    """Найти JSON массивы [] в тексте."""
    candidates = []
    bracket_count = 0
    start_pos = -1

    for i, char in enumerate(text):
        if char == "[":
            if bracket_count == 0:
                start_pos = i
            bracket_count += 1
        elif char == "]":
            bracket_count -= 1
            if bracket_count == 0 and start_pos != -1:
                candidate = text[start_pos : i + 1]
                candidates.append(candidate)
                start_pos = -1

    return candidates


def _validate_json_candidates(candidates: list) -> str | None:
    """Проверить кандидатов на валидность JSON."""
    for candidate in candidates:
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            continue
    return None


def safe_extract_json(text: str) -> str:
    """
    Попытка извлечь JSON-объект или массив из текста ответа LLM.
    Поддерживает как одиночные объекты {}, так и массивы [{}].

    :param text: Текст для парсинга
    :return: Результат парсинга
    """

    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()

    candidates = []
    candidates.extend(_find_json_objects(text))
    candidates.extend(_find_json_arrays(text))

    result = _validate_json_candidates(candidates)
    if result:
        return result

    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass

    raise ValueError("Не удалось найти валидный JSON объект или массив")
