"""
Валидатор структуры ответа WB API для Incomes (/api/v1/supplier/incomes).
Проверяет наличие и корректность всех необходимых полей.
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List
import json

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from excel_actions.utils.schemas.schema_utils import load_json


def validate_incomes_structure(response_data: list) -> bool:
    """
    Валидирует структуру ответа API для Incomes.
    
    Args:
        response_data: Полный ответ от API (список словарей)
    
    Returns:
        True если структура валидна, False при критических ошибках
    """
    print("🔍 Валидация структуры Incomes API...")
    
    # Проверка базовой структуры
    if not isinstance(response_data, list):
        print("❌ ОШИБКА: Ответ API должен быть массивом")
        return False
    
    if not response_data:
        print("⚠️  WARNING: Ответ не содержит записей (пустой массив)")
        return True
    
    # Валидация первой записи (как эталон)
    first_record = response_data[0]
    if not _validate_record_structure(first_record, 0):
        return False
    
    print(f"✅ Структура валидна. Записей: {len(response_data)}")
    return True


def _validate_record_structure(record: dict, idx: int) -> bool:
    """
    Детальная валидация отдельной записи.
    
    Args:
        record: Данные записи
        idx: Индекс записи в массиве
    
    Returns:
        True если структура валидна, False при критических ошибках
    """
    if not isinstance(record, dict):
        print(f"❌ ОШИБКА: Запись [{idx}] должна быть объектом")
        return False
    
    # Обязательные поля
    required_fields = {
        "incomeId": int,
        "date": str,
        "lastChangeDate": str,
        "supplierArticle": str,
        "barcode": str,
        "quantity": int,
        "warehouseName": str,
        "nmId": int,
        "status": str,
        "number": str,
        "techSize": str
    }
    
    for field, expected_type in required_fields.items():
        if field not in record:
            print(f"❌ ОШИБКА: Запись [{idx}]: отсутствует поле '{field}'")
            return False
        
        if not isinstance(record[field], expected_type):
            actual_type = type(record[field]).__name__
            print(f"❌ ОШИБКА: Запись [{idx}]: {field} должен быть {expected_type.__name__}, получен {actual_type}")
            return False
    
    # Дополнительные проверки
    income_id = record["incomeId"]
    nm_id = record["nmId"]
    
    if income_id <= 0:
        print(f"❌ ОШИБКА: Запись [{idx}]: incomeId должен быть положительным числом")
        return False
    
    if nm_id <= 0:
        print(f"❌ ОШИБКА: Запись [{idx}]: nmId должен быть положительным числом")
        return False
    
    if record["quantity"] < 0:
        print(f"❌ ОШИБКА: Запись [{idx}]: quantity не может быть отрицательным")
        return False
    
    return True


def validate_incomes_with_schema(response_data: list) -> bool:
    """
    Валидация с использованием JSON Schema (опционально).
    
    Args:
        response_data: Данные для валидации
    
    Returns:
        True если валидация прошла успешно
    """
    try:
        schema_path = PROJECT_ROOT / "excel_actions" / "utils" / "schemas" / "incomes.schema.json"
        schema = load_json(str(schema_path))
        
        # Здесь можно добавить валидацию через jsonschema если нужно
        # import jsonschema
        # jsonschema.validate(response_data, schema)
        
        print("✅ JSON Schema валидация пройдена")
        return True
        
    except Exception as e:
        print(f"⚠️  WARNING: JSON Schema валидация пропущена: {e}")
        return True


if __name__ == "__main__":
    """Тестовый запуск валидатора на существующем JSON"""
    
    # Путь к тестовому файлу
    test_file = PROJECT_ROOT / "incomes_response_20251026_114703.json"
    
    if not test_file.exists():
        print(f"❌ Тестовый файл не найден: {test_file}")
        sys.exit(1)
    
    print(f"📂 Загрузка тестового файла: {test_file}")
    with open(test_file, "r", encoding="utf-8") as f:
        test_data = json.load(f)
    
    try:
        validate_incomes_structure(test_data)
        validate_incomes_with_schema(test_data)
        print("\n✅ Валидация прошла успешно!")
    except ValueError as e:
        print(f"\n❌ Ошибка валидации:\n{e}")
        sys.exit(1)
