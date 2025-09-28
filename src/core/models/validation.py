from dataclasses import dataclass
from typing import List


@dataclass
class ValidationResult:
    """Результат валидации."""

    is_valid: bool
    errors: List[str]
    warnings: List[str] = None
