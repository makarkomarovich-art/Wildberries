"""
Валидация структуры ответа WB API для CR Daily Stats (nm-report/detail).
Проверяет наличие и корректность всех необходимых полей.

Ожидаем:
- data.cards[] - массив карточек
  - nmID: int (обязательно)
  - statistics.selectedPeriod: объект с метриками (обязательно)
  - statistics.previousPeriod: объект с метриками (обязательно)
  - statistics.*.conversions: объект с процентами конверсий (обязательно)
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def validate_cr_daily_stats_structure(response_data: dict) -> bool:
    """
    Валидирует структуру ответа API для CR Daily Stats.
    
    Args:
        response_data: Полный ответ от API (словарь)
    
    Returns:
        True если структура валидна, False при критических ошибках
    """
    print("🔍 Валидация структуры CR Daily Stats...")
    
    # Проверка базовой структуры
    if not isinstance(response_data, dict):
        print("❌ ОШИБКА: Ответ API должен быть объектом")
        return False
    
    if "data" not in response_data:
        print("❌ ОШИБКА: Отсутствует ключ 'data' в ответе")
        return False
    
    data = response_data["data"]
    if not isinstance(data, dict):
        print("❌ ОШИБКА: 'data' должен быть объектом")
        return False
    
    if "cards" not in data:
        print("❌ ОШИБКА: Отсутствует ключ 'cards' в data")
        return False
    
    cards = data["cards"]
    if not isinstance(cards, list):
        print("❌ ОШИБКА: 'cards' должен быть массивом")
        return False
    
    if not cards:
        print("⚠️  WARNING: Ответ не содержит карточек (cards пустой)")
        return True
    
    # Валидация первой карточки (как эталон)
    first_card = cards[0]
    if not _validate_card_structure(first_card, 0):
        return False
    
    print(f"✅ Структура валидна. Карточек: {len(cards)}")
    return True


def _validate_card_structure(card: dict, idx: int) -> bool:
    """
    Детальная валидация отдельной карточки.
    
    Args:
        card: Данные карточки
        idx: Индекс карточки в массиве
    
    Returns:
        True если структура валидна, False при критических ошибках
    """
    if not isinstance(card, dict):
        print(f"❌ ОШИБКА: Карточка [{idx}] должна быть объектом")
        return False
    
    # Проверка nmID
    if "nmID" not in card:
        print(f"❌ ОШИБКА: Карточка [{idx}]: отсутствует поле 'nmID'")
        return False
    
    if not isinstance(card["nmID"], int):
        print(f"❌ ОШИБКА: Карточка [{idx}]: nmID должен быть integer, получен {type(card['nmID']).__name__}")
        return False
    
    nm_id = card["nmID"]
    
    # Проверка vendorCode
    if "vendorCode" not in card:
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): отсутствует поле 'vendorCode'")
        return False
    
    if not isinstance(card["vendorCode"], str):
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): vendorCode должен быть string, получен {type(card['vendorCode']).__name__}")
        return False
    
    # Проверка statistics
    if "statistics" not in card:
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): отсутствует поле 'statistics'")
        return False
    
    stats = card["statistics"]
    if not isinstance(stats, dict):
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): statistics должен быть объектом")
        return False
    
    # Проверка selectedPeriod
    if "selectedPeriod" not in stats:
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): отсутствует statistics.selectedPeriod")
        return False
    
    if not _validate_period(stats["selectedPeriod"], "selectedPeriod", nm_id, idx):
        return False
    
    # Проверка previousPeriod
    if "previousPeriod" not in stats:
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): отсутствует statistics.previousPeriod")
        return False
    
    if not _validate_period(stats["previousPeriod"], "previousPeriod", nm_id, idx):
        return False
    
    return True


def _validate_period(period: dict, period_name: str, nm_id: int, idx: int) -> bool:
    """
    Валидация данных периода (selectedPeriod или previousPeriod).
    
    Args:
        period: Данные периода
        period_name: Название периода (для сообщений об ошибках)
        nm_id: nmID карточки
        idx: Индекс карточки
    
    Returns:
        True если структура валидна, False при критических ошибках
    """
    if not isinstance(period, dict):
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): {period_name} должен быть объектом")
        return False
    
    required_fields = [
        "openCardCount",
        "addToCartCount",
        "ordersCount",
        "ordersSumRub",
    ]
    
    for field in required_fields:
        if field not in period:
            print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): {period_name}.{field} отсутствует")
            return False
    
    # Проверка conversions
    if "conversions" not in period:
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): {period_name}.conversions отсутствует")
        return False
    
    conversions = period["conversions"]
    if not isinstance(conversions, dict):
        print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): {period_name}.conversions должен быть объектом")
        return False
    
    conv_fields = ["addToCartPercent", "cartToOrderPercent"]
    for field in conv_fields:
        if field not in conversions:
            print(f"❌ ОШИБКА: Карточка [{idx}] (nmID={nm_id}): {period_name}.conversions.{field} отсутствует")
            return False
    
    return True


if __name__ == "__main__":
    """Тестовый запуск валидатора на существующем JSON"""
    import json
    
    # Путь к тестовому файлу
    test_file = PROJECT_ROOT / "test_new_methods" / "CR_big_2025-09-27_to_2025-10-04.json"
    
    if not test_file.exists():
        print(f"❌ Тестовый файл не найден: {test_file}")
        sys.exit(1)
    
    print(f"📂 Загрузка тестового файла: {test_file}")
    with open(test_file, "r", encoding="utf-8") as f:
        test_data = json.load(f)
    
    try:
        validate_cr_daily_stats_structure(test_data)
        print("\n✅ Валидация прошла успешно!")
    except ValueError as e:
        print(f"\n❌ Ошибка валидации:\n{e}")
        sys.exit(1)

