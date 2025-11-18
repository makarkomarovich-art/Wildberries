"""
Pre-валидация ответа для /products/history.
Ожидаем: {"data": [ { "product": {...}, "history": [...] }, ... ]}
"""
from __future__ import annotations
from typing import Any, Dict, List


def validate_history_structure(response_data: dict) -> bool:
    print("🔍 Валидация структуры CR History...")
    if not isinstance(response_data, dict):
        print("❌ ОШИБКА: Ответ должен быть объектом")
        return False
    if "data" not in response_data:
        print("❌ ОШИБКА: Нет ключа 'data'")
        return False
    items = response_data["data"]
    if not isinstance(items, list):
        print("❌ ОШИБКА: 'data' должен быть массивом")
        return False
    if not items:
        print("⚠️  WARNING: Пустой массив data")
        return True
    first = items[0]
    if not isinstance(first, dict):
        print("❌ ОШИБКА: data[0] должен быть объектом")
        return False
    if "product" not in first:
        print("❌ ОШИБКА: data[0] без 'product'")
        return False
    if "history" not in first:
        print("❌ ОШИБКА: data[0] без 'history'")
        return False
    prod = first["product"]
    hist = first["history"]
    if not isinstance(prod, dict):
        print("❌ ОШИБКА: 'product' должен быть объектом")
        return False
    if not isinstance(hist, list):
        print("❌ ОШИБКА: 'history' должен быть массивом")
        return False
    print(f"✅ Структура валидна. Элементов: {len(items)}")
    return True


