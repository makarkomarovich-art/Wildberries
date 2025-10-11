#!/usr/bin/env python3
"""
Валидация структуры API-ответов через JSON Schema.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import jsonschema
from jsonschema import Draft7Validator


# Пути к схемам
SCHEMA_DIR = Path(__file__).parent.parent / "utils" / "schemas"
PROMOTION_COUNT_SCHEMA = SCHEMA_DIR / "adv_promotion_count.schema.json"
FULLSTATS_SCHEMA = SCHEMA_DIR / "adv_fullstats.schema.json"


class ValidationError(Exception):
    """Ошибка валидации данных."""
    pass


def load_schema(schema_path: Path) -> Dict[str, Any]:
    """Загрузить JSON Schema из файла."""
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    
    with schema_path.open('r', encoding='utf-8') as f:
        return json.load(f)


def validate_promotion_count_response(response: Dict[str, Any]) -> None:
    """
    Валидировать ответ API /adv/v1/promotion/count.
    
    Args:
        response: Ответ от API
    
    Raises:
        ValidationError: При ошибке валидации
    """
    schema = load_schema(PROMOTION_COUNT_SCHEMA)
    
    try:
        jsonschema.validate(response, schema)
    except jsonschema.ValidationError as e:
        raise ValidationError(f"Promotion count response validation failed: {e.message}") from e


def validate_fullstats_response(response: List[Dict[str, Any]]) -> None:
    """
    Валидировать ответ API /adv/v3/fullstats.
    
    Args:
        response: Ответ от API (список кампаний)
    
    Raises:
        ValidationError: При ошибке валидации
    """
    schema = load_schema(FULLSTATS_SCHEMA)
    
    try:
        jsonschema.validate(response, schema)
    except jsonschema.ValidationError as e:
        raise ValidationError(f"Fullstats response validation failed: {e.message}") from e


def get_validation_report(response: Any, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Получить детальный отчет о валидации (все ошибки).
    
    Args:
        response: Данные для проверки
        schema: JSON Schema
    
    Returns:
        Dict с полями:
            - valid: bool
            - errors: List[str] (список всех ошибок)
            - error_count: int
    """
    validator = Draft7Validator(schema)
    errors = list(validator.iter_errors(response))
    
    return {
        'valid': len(errors) == 0,
        'errors': [e.message for e in errors],
        'error_count': len(errors)
    }

