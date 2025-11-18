"""
Валидация структуры конфигурации и Google Sheets.
"""

import logging
from typing import Dict, List, Tuple
from .header_config import HEADER_MAPPING, SHEET_NAME


def validate_header_config() -> Tuple[bool, List[str]]:
    """
    Валидирует header_config.py на корректность.
    
    Returns:
        (is_valid, list_of_errors)
    """
    errors = []
    
    if not HEADER_MAPPING:
        errors.append("HEADER_MAPPING пуст")
        return False, errors
    
    # Проверяем, что все колонки идут по порядку (A, B, C, ...)
    expected_columns = [chr(65 + i) for i in range(len(HEADER_MAPPING))]
    actual_columns = list(HEADER_MAPPING.keys())
    
    if actual_columns != expected_columns:
        errors.append(f"Колонки не по порядку. Ожидается: {expected_columns}, получено: {actual_columns}")
        return False, errors
    
    # Проверяем обязательные поля в каждом хедере
    required_fields = ["name", "source", "type"]
    for col, config in HEADER_MAPPING.items():
        for field in required_fields:
            if field not in config:
                errors.append(f"Колонка {col} ({config.get('name', '?')}): отсутствует поле '{field}'")
    
    # Проверяем, что "type" имеет допустимое значение
    valid_types = ["dimension", "metric"]
    for col, config in HEADER_MAPPING.items():
        if config.get("type") not in valid_types:
            errors.append(f"Колонка {col}: неверный type '{config.get('type')}'. Допустимо: {valid_types}")
    
    if errors:
        return False, errors
    
    logging.info("✅ Конфигурация хедеров валидна")
    return True, []


def validate_google_sheet_headers(google_headers: List[str]) -> Tuple[bool, List[str]]:
    """
    Валидирует хедеры из Google Sheets на соответствие конфигурации.
    
    Args:
        google_headers: Список хедеров из Google (строка 1)
    
    Returns:
        (is_valid, list_of_errors)
    """
    errors = []
    
    if not google_headers:
        errors.append("Хедеры из Google Sheets пусты")
        return False, errors
    
    # Проверяем количество колонок
    if len(google_headers) < len(HEADER_MAPPING):
        errors.append(
            f"Недостаточно колонок в Google Sheets. "
            f"Ожидается: {len(HEADER_MAPPING)}, получено: {len(google_headers)}"
        )
        return False, errors
    
    # Проверяем соответствие названий хедеров
    for i, (col_letter, expected_config) in enumerate(HEADER_MAPPING.items()):
        if i < len(google_headers):
            actual_name = google_headers[i].strip() if google_headers[i] else ""
            expected_name = expected_config["name"]
            
            if actual_name != expected_name:
                errors.append(
                    f"Колонка {col_letter}: неверный хедер. "
                    f"Ожидается '{expected_name}', получено '{actual_name}'"
                )
    
    if errors:
        logging.error("❌ Ошибки валидации Google Sheets хедеров:")
        for err in errors:
            logging.error(f"   - {err}")
        return False, errors
    
    logging.info("✅ Хедеры Google Sheets валидны")
    return True, []

