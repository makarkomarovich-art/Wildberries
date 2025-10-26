"""
Валидация структуры данных orders API.
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple
import json
import sys
import importlib.util

# Динамически импортируем schema_utils
_SCHEMAS_DIR = Path(__file__).parent.parent / "utils" / "schemas"
_SCHEMA_UTILS_PATH = _SCHEMAS_DIR / "schema_utils.py"
_spec = importlib.util.spec_from_file_location("schema_utils", str(_SCHEMA_UTILS_PATH))
schema_utils = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(schema_utils)

load_json = schema_utils.load_json
validate_flexible_type = schema_utils.validate_flexible_type


def validate_basic_structure(item: Dict[str, Any], item_index: int = 0) -> Tuple[bool, str]:
    """
    Проверяет основные поля заказа.
    
    Args:
        item: Элемент из orders API
        item_index: Индекс элемента для отчета
        
    Returns:
        tuple[bool, str]: (True если корректна, детальная информация об ошибках)
    """
    
    # Загружаем схему
    schema_path = _SCHEMAS_DIR / "orders.schema.json"
    schema = load_json(str(schema_path))
    item_structure = schema["item_structure"]
    critical_fields = schema["critical_fields"]
    optional_fields = schema["optional_fields"]
    
    errors = []
    warnings = []
    
    # Проверяем критические поля
    for field in critical_fields:
        if field not in item:
            errors.append(f"Элемент {item_index}: отсутствует критическое поле '{field}'")
            continue
            
        value = item[field]
        expected_type = item_structure[field]
        
        if not validate_flexible_type(value, expected_type):
            actual_type = type(value).__name__ if value is not None else 'null'
            errors.append(f"Элемент {item_index}: поле '{field}' имеет неправильный тип. Ожидается {expected_type}, получен {actual_type}")
    
    # Проверяем опциональные поля (только предупреждения)
    for field in optional_fields:
        if field not in item:
            warnings.append(f"Элемент {item_index}: отсутствует опциональное поле '{field}'")
        else:
            value = item[field]
            expected_type = item_structure[field]
            
            if not validate_flexible_type(value, expected_type):
                actual_type = type(value).__name__ if value is not None else 'null'
                warnings.append(f"Элемент {item_index}: поле '{field}' имеет неожиданный тип. Ожидается {expected_type}, получен {actual_type}")
    
    # Формируем результат
    result_messages = []
    if errors:
        result_messages.extend(errors)
    if warnings:
        result_messages.extend([f"⚠️ {w}" for w in warnings])
    
    return len(errors) == 0, "\n".join(result_messages)


def handle_structure_change(changes_info: str = "") -> bool:
    """
    Обрабатывает изменения структуры данных.
    
    Args:
        changes_info: Информация об изменениях
        
    Returns:
        bool: True если можно продолжать, False если нужно остановить
    """
    print("\n❌ Обнаружены изменения в структуре данных")
    print("=" * 60)
    print(changes_info)
    print("=" * 60)
    
    choice = input("\nЧто делать?\n1. Продолжить (игнорировать ошибки)\n2. Остановить\nВыбор (1/2): ")
    
    if choice == "1":
        print("⚠️  Продолжаем выполнение с ошибками валидации")
        return True
    elif choice == "2":
        print("🛑 Остановка выполнения")
        return False
    else:
        print("❌ Неверный выбор. Введите 1 или 2.")
        return handle_structure_change(changes_info)


def check_and_validate_structure(data: List[Dict[str, Any]]) -> bool:
    """
    Основная функция валидации структуры orders.
    
    Args:
        data: Данные от orders API
        
    Returns:
        bool: True если можно продолжать, False если нужно остановить
    """
    print("🔍 Проверяем структуру отчёта orders...")
    
    if not isinstance(data, list):
        error_info = "Данные должны быть списком"
        print(f"\n❌ {error_info}")
        return handle_structure_change(error_info)
    
    if not data:
        print("⚠️ Пустой список заказов — нечего валидировать")
        return True
    
    # Проверяем каждый элемент
    critical_errors = []
    warnings = []
    
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            critical_errors.append(f"Элемент {i}: не является объектом (dict), получен {type(item).__name__}")
            continue
        
        # Проверяем базовую структуру
        is_valid_basic, basic_info = validate_basic_structure(item, i)
        if not is_valid_basic:
            critical_errors.append(basic_info)
        elif basic_info:  # Есть предупреждения
            warnings.append(basic_info)
    
    # Если есть критические ошибки - останавливаемся
    if critical_errors:
        print("\n❌ Критические ошибки в структуре данных:")
        error_info = "\n".join(critical_errors[:10])  # Показываем только первые 10
        if len(critical_errors) > 10:
            error_info += f"\n... и ещё {len(critical_errors) - 10} ошибок"
        print(error_info)
        return handle_structure_change(error_info)
    
    # Если есть только предупреждения - выводим их, но продолжаем
    if warnings:
        print("\n⚠️ Предупреждения (не критично):")
        for warning in warnings[:10]:  # Показываем только первые 10
            print(f"  • {warning}")
        if len(warnings) > 10:
            print(f"  ... и ещё {len(warnings) - 10} предупреждений")
    
    print("✅ Структура синхронизирована!")
    return True
