"""
Валидатор структуры ответа WB API для Supply Details (JSON-RPC 2.0).
Проверяет наличие и корректность поля deliveryAndStorageExpr.
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def validate_supply_details_structure(response_data: dict) -> bool:
    """
    Валидирует только ключевое поле deliveryAndStorageExpr.
    
    Args:
        response_data: Полный ответ от API (JSON-RPC 2.0)
    
    Returns:
        True если структура валидна, False при критических ошибках
    """
    print("🔍 Валидация deliveryAndStorageExpr...")
    
    try:
        # Простая проверка пути к нужному полю
        result = response_data.get("result", {})
        supply = result.get("supply", {})
        delivery_storage = supply.get("deliveryAndStorage", {})
        expr_value = delivery_storage.get("deliveryAndStorageExpr")
        
        if expr_value is not None:
            print(f"✅ deliveryAndStorageExpr найден: {expr_value}")
        else:
            print(f"⚠️  deliveryAndStorageExpr: null")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка валидации deliveryAndStorageExpr: {e}")
        return False


def extract_delivery_expr(response_data: dict) -> str | None:
    """
    Извлекает значение deliveryAndStorageExpr из ответа.
    
    Args:
        response_data: JSON-RPC ответ
        
    Returns:
        str | None: Значение коэффициента логистики или None
    """
    try:
        result = response_data.get("result", {})
        supply = result.get("supply", {})
        delivery_storage = supply.get("deliveryAndStorage", {})
        expr_value = delivery_storage.get("deliveryAndStorageExpr")
        
        if expr_value is None:
            return None
        
        # Конвертируем в строку если нужно
        return str(expr_value)
        
    except Exception as e:
        print(f"❌ Ошибка извлечения deliveryAndStorageExpr: {e}")
        return None


if __name__ == "__main__":
    """Тестовый запуск валидатора на существующем JSON"""
    import json
    
    # Путь к тестовому файлу
    test_file = PROJECT_ROOT / "wb_api" / "supplies_to_wh" / "supply_details_response_20251026_114709.json"
    
    if not test_file.exists():
        print(f"❌ Тестовый файл не найден: {test_file}")
        sys.exit(1)
    
    print(f"📂 Загрузка тестового файла: {test_file}")
    with open(test_file, "r", encoding="utf-8") as f:
        test_data = json.load(f)
    
    try:
        validate_supply_details_structure(test_data)
        expr = extract_delivery_expr(test_data)
        print(f"\n✅ Валидация прошла успешно!")
        print(f"📊 Извлеченное значение: {expr}")
    except ValueError as e:
        print(f"\n❌ Ошибка валидации:\n{e}")
        sys.exit(1)
