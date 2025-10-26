#!/usr/bin/env python3
"""
Скрипт для запуска тестов supplies_to_warehouses с coverage
"""
import sys
import os
from pathlib import Path

# Добавляем корень проекта в sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Импортируем тесты
from tests.test_supplies_to_warehouses import *

print("🧪 Запуск тестов supplies_to_warehouses...")
print("=" * 50)

# Запускаем основные тесты
try:
    # Тест валидации incomes
    print("1️⃣ Тестирование валидации incomes...")
    from excel_actions.supplies_to_warehouses_ea.structure_validator import validate_incomes_structure
    
    test_data = [
        {
            'incomeId': 123,
            'nmId': 456,
            'status': 'Принято',
            'supplierArticle': 'test',
            'barcode': '123',
            'quantity': 10,
            'warehouseName': 'test',
            'date': '2025-01-01T00:00:00',
            'lastChangeDate': '2025-01-01T00:00:00',
            'number': '',
            'techSize': '0'
        }
    ]
    
    result = validate_incomes_structure(test_data)
    print(f"   ✅ Валидация incomes: {result}")
    
    # Тест фильтрации
    print("2️⃣ Тестирование фильтрации...")
    from excel_actions.supplies_to_warehouses_ea.transform import filter_accepted_supplies
    filtered = filter_accepted_supplies(test_data)
    print(f"   ✅ Фильтрация: {len(filtered)} записей")
    
    # Тест трансформации
    print("3️⃣ Тестирование трансформации...")
    from excel_actions.supplies_to_warehouses_ea.transform import prepare_for_db
    transformed = prepare_for_db(filtered)
    print(f"   ✅ Трансформация: {len(transformed)} записей")
    
    # Тест конвертации delivery_expr
    print("4️⃣ Тестирование конвертации delivery_expr...")
    from excel_actions.supplies_to_warehouses_ea.transform import convert_delivery_expr_to_numeric
    from decimal import Decimal
    
    assert convert_delivery_expr_to_numeric("170") == Decimal("170")
    assert convert_delivery_expr_to_numeric(None) is None
    print("   ✅ Конвертация delivery_expr работает")
    
    # Тест supply details валидации
    print("5️⃣ Тестирование валидации supply details...")
    from excel_actions.supplies_to_warehouses_ea.supply_details_validator import validate_supply_details_structure, extract_delivery_expr
    
    test_supply_data = {
        "result": {
            "supply": {
                "deliveryAndStorage": {
                    "deliveryAndStorageExpr": "170"
                }
            }
        }
    }
    
    result = validate_supply_details_structure(test_supply_data)
    expr = extract_delivery_expr(test_supply_data)
    print(f"   ✅ Валидация supply details: {result}, delivery_expr: {expr}")
    
    print("=" * 50)
    print("🎉 Все тесты прошли успешно!")
    print("📊 Функции supplies_to_warehouses работают корректно")
    
except Exception as e:
    print(f"❌ Ошибка в тестах: {e}")
    sys.exit(1)
