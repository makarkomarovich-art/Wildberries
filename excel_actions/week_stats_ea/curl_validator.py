"""
Валидатор для CURL API.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict

import jsonschema
from jsonschema import Draft7Validator


logger = logging.getLogger(__name__)

# Путь к схеме
SCHEMA_DIR = Path(__file__).parent.parent / "utils" / "schemas"
CURL_SCHEMA = SCHEMA_DIR / "week_stats_curl_schema.json"


class ValidationError(Exception):
    """Ошибка валидации данных."""
    pass


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """Загрузить JSON Schema из файла."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with schema_path.open('r', encoding='utf-8') as f:
        return json.load(f)


def validate_curl_data(curl_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валидирует данные из CURL API.
    
    Args:
        curl_data: Данные из CURL API
        
    Returns:
        Dict с полями:
            - valid: bool
            - errors: List[str]
            - error_count: int
    """
    schema = load_schema(CURL_SCHEMA)
    validator = Draft7Validator(schema)
    errors = list(validator.iter_errors(curl_data))
    
    result = {
        'valid': len(errors) == 0,
        'errors': [e.message for e in errors],
        'error_count': len(errors)
    }
    
    return result
