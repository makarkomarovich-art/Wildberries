"""
Строгая проверка структуры для базы артикулов из Content API (cards list).

Ожидаем: список карточек (list[dict]) с полями на верхнем уровне:
- nmID: int (обязательно)
- imtID: int (обязательно)
- subjectName: string (обязательно)
- vendorCode: string (обязательно)
- title: string (обязательно)
- sizes: list, где в sizes[i] есть поля:
  - techSize: string (опционально, WARNING если нет)
  - skus: list (обязательно, ERROR если пусто)
"""

from typing import Any, Dict, List


def validate_report_structure(rows: List[Dict[str, Any]]) -> bool:
    """
    Валидация структуры карточек из Content API.
    
    Обязательные поля: nmID, imtID, subjectName, vendorCode, title, sizes[].skus
    Опциональные поля: sizes[].techSize (WARNING если нет)
    
    Returns:
        bool: True если структура корректна, False если критическая ошибка
    """
    print("🔍 Проверяем структуру карточек (Content API)")
    
    if not isinstance(rows, list):
        print("❌ ОШИБКА: Данные отчёта должны быть списком")
        return False
    
    if not rows:
        print("⚠️ WARNING: Пустой отчёт")
        return False

    first = rows[0]
    if not isinstance(first, dict):
        print("❌ ОШИБКА: Первая запись не объект")
        return False

    # Проверяем обязательные поля на верхнем уровне
    required_fields = {
        'nmID': int,
        'imtID': int,
        'subjectName': str,
        'vendorCode': str,
        'title': str,
        'sizes': list
    }
    
    for field, expected_type in required_fields.items():
        if field not in first:
            print(f"❌ ОШИБКА: Нет поля '{field}' на верхнем уровне")
            print(f"   Доступные поля: {sorted(first.keys())}")
            return False
        
        if not isinstance(first[field], expected_type):
            print(f"❌ ОШИБКА: Поле '{field}' должно быть {expected_type.__name__}, "
                  f"получен {type(first[field]).__name__}")
            return False

    # Проверяем sizes[].techSize и sizes[].skus
    sizes = first['sizes']
    if not sizes:
        print("❌ ОШИБКА: Массив 'sizes' пустой")
        return False
    
    s0 = sizes[0]
    if not isinstance(s0, dict):
        print("❌ ОШИБКА: sizes[0] должен быть объектом")
        return False
    
    # techSize - опционально, WARNING если нет
    if 'techSize' not in s0:
        print("⚠️ WARNING: В sizes[0] отсутствует поле 'techSize' (будет использована пустая строка)")
    elif not isinstance(s0['techSize'], str):
        print(f"⚠️ WARNING: Поле 'techSize' должно быть string, получен {type(s0['techSize']).__name__}")
    
    # skus - обязательно
    if 'skus' not in s0:
        print("❌ ОШИБКА: В sizes[0] нет поля 'skus'")
        return False
    
    if not isinstance(s0['skus'], list):
        print("❌ ОШИБКА: Поле 'skus' должно быть list")
        return False
    
    skus = s0['skus']
    if not skus:
        print("❌ ОШИБКА: Массив 'skus' пустой (нет баркодов)")
        return False
    
    if not isinstance(skus[0], str):
        print(f"❌ ОШИБКА: skus[0] должен быть строкой (баркод), получен {type(skus[0]).__name__}")
        return False

    print("✅ Структура корректна (nmID, imtID, subjectName, vendorCode, title, sizes[].techSize, sizes[].skus)")
    return True


def check_and_validate_structure(rows: List[Dict[str, Any]]) -> bool:
    """Обёртка для совместимости стиля вызова"""
    return validate_report_structure(rows)
