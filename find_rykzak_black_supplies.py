#!/usr/bin/env python3
"""
Скрипт для поиска поставок с артикулом "rykzak_black".
Использует API /api/v1/supplier/incomes для получения товаров в поставках.
"""
import json
import time
import requests
from collections import defaultdict
from typing import Dict, List, Optional, Set
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
import sys
import os

# Добавляем корень проекта в путь
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import api_keys

# Настройка API для /incomes
INCOMES_BASE_URL = "https://statistics-api.wildberries.ru"
INCOMES_ENDPOINT = "/api/v1/supplier/incomes"
TARGET_ARTICLE = "rykzak_black"

# Получаем API ключ для KUSKOV
ACTIVE_USER = "KUSKOV"
user_config = api_keys.USERS.get(ACTIVE_USER, api_keys.USERS["KUSKOV"])
WB_API_TOKEN = user_config["WB_API_TOKEN"]

HEADERS = {
    "Authorization": WB_API_TOKEN,
    "Content-Type": "application/json",
}

# Timezone
TZ_NAME = "Europe/Moscow"
tz = ZoneInfo(TZ_NAME)

# Rate limiting для /incomes: 1 запрос в минуту
RATE_LIMIT_DELAY = 60  # секунды


def load_supply_ids(file_path: str) -> tuple[List[int], str]:
    """
    Загружает supplyID из JSON файла и определяет самую раннюю дату.
    
    Returns:
        tuple: (список supplyID, самая ранняя дата в формате RFC3339)
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    supply_ids = [item["supplyID"] for item in data if "supplyID" in item]
    
    # Находим самую раннюю дату создания поставки
    earliest_date = None
    for item in data:
        if "createDate" in item:
            create_date_str = item["createDate"]
            # Парсим дату (формат: "2025-06-22T13:29:10+03:00")
            try:
                create_date = datetime.fromisoformat(create_date_str.replace('+03:00', '+03:00'))
                if earliest_date is None or create_date < earliest_date:
                    earliest_date = create_date
            except:
                pass
    
    # Если не нашли дату, используем дату 6 месяцев назад
    if earliest_date is None:
        earliest_date = datetime.now(tz) - timedelta(days=180)
    
    # Форматируем дату для API (RFC3339)
    date_from = earliest_date.strftime("%Y-%m-%dT00:00:00")
    
    print(f"✅ Загружено {len(supply_ids)} supplyID из файла")
    print(f"📅 Самая ранняя дата поставки: {earliest_date.strftime('%Y-%m-%d')}")
    print(f"📅 Дата начала загрузки: {date_from}")
    
    return supply_ids, date_from


def fetch_incomes_page(date_from: str) -> tuple[List[Dict], Optional[str]]:
    """
    Загружает одну страницу данных поставок из /incomes API.
    
    Args:
        date_from: Дата начала в формате RFC3339
        
    Returns:
        tuple: (список записей, lastChangeDate последней записи или None)
    """
    url = INCOMES_BASE_URL + INCOMES_ENDPOINT
    params = {"dateFrom": date_from}
    
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)
        
        if response.status_code == 429:
            print(f"⚠️  Rate limit. Ожидание 60 секунд...")
            time.sleep(60)
            response = requests.get(url, headers=HEADERS, params=params, timeout=60)
        
        if response.status_code == 401:
            print(f"❌ Ошибка авторизации (401)")
            return [], None
        
        if response.status_code != 200:
            print(f"⚠️  Ошибка {response.status_code}: {response.text[:200]}")
            return [], None
        
        data = response.json()
        
        if not isinstance(data, list):
            print(f"⚠️  Ожидался массив, получен {type(data).__name__}")
            return [], None
        
        if data:
            last_change_date = data[-1].get('lastChangeDate')
            return data, last_change_date
        else:
            return [], None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")
        return [], None
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return [], None


def fetch_all_incomes(date_from: str, target_supply_ids: Set[int]) -> List[Dict]:
    """
    Загружает все данные поставок с пагинацией и фильтрует по target_supply_ids.
    
    Args:
        date_from: Дата начала в формате RFC3339
        target_supply_ids: Множество supplyID для фильтрации
        
    Returns:
        list: Список записей поставок, отфильтрованных по target_supply_ids
    """
    all_records = []
    current_date = date_from
    page_count = 0
    found_supply_ids = set()
    
    print(f"\n📡 Загрузка данных из /incomes API...")
    print(f"🎯 Ищем поставки: {sorted(list(target_supply_ids))[:10]}{'...' if len(target_supply_ids) > 10 else ''}")
    
    while True:
        page_count += 1
        print(f"\n📄 Страница {page_count}")
        
        # Задержка между запросами (кроме первого)
        if page_count > 1:
            print(f"⏳ Ожидание {RATE_LIMIT_DELAY} секунд...")
            time.sleep(RATE_LIMIT_DELAY)
        
        records, last_change_date = fetch_incomes_page(current_date)
        
        if not records:
            print(f"✅ Пагинация завершена. Всего записей: {len(all_records)}")
            break
        
        # Фильтруем по target_supply_ids
        filtered_records = [r for r in records if r.get('incomeId') in target_supply_ids]
        all_records.extend(filtered_records)
        
        # Отслеживаем найденные supplyID
        for record in filtered_records:
            found_supply_ids.add(record.get('incomeId'))
        
        print(f"   Получено записей: {len(records)}, отфильтровано: {len(filtered_records)}")
        print(f"   Найдено поставок: {len(found_supply_ids)}/{len(target_supply_ids)}")
        
        # Если нашли все поставки, можно остановиться
        if len(found_supply_ids) >= len(target_supply_ids):
            print(f"✅ Все целевые поставки найдены!")
            break
        
        # Если получено меньше 100000 записей - конец пагинации
        if len(records) < 100000:
            print(f"✅ Пагинация завершена. Всего записей: {len(all_records)}")
            break
        
        # Обновляем дату для следующего запроса
        if last_change_date:
            current_date = last_change_date
        else:
            print(f"⚠️  Нет lastChangeDate для продолжения пагинации")
            break
    
    return all_records


def main():
    """Основная функция."""
    print("=" * 80)
    print("🔍 Поиск поставок с артикулом 'rykzak_black'")
    print("=" * 80)
    
    # Загружаем supplyID из файла
    supply_file = "/Users/makar/Desktop/поставки.txt"
    supply_ids, date_from = load_supply_ids(supply_file)
    supply_ids_set = set(supply_ids)
    
    print(f"\n📋 Всего поставок для проверки: {len(supply_ids)}")
    print(f"🎯 Ищем артикул: {TARGET_ARTICLE}\n")
    
    # Загружаем все данные поставок из /incomes API
    all_incomes = fetch_all_incomes(date_from, supply_ids_set)
    
    if not all_incomes:
        print("\n⚠️  Не удалось загрузить данные поставок")
        return
    
    print(f"\n✅ Загружено {len(all_incomes)} записей товаров в поставках")
    
    # Фильтруем по артикулу "rykzak_black"
    rykzak_black_records = [
        record for record in all_incomes
        if record.get('supplierArticle', '').lower() == TARGET_ARTICLE.lower()
    ]
    
    print(f"\n🎯 Найдено записей с артикулом '{TARGET_ARTICLE}': {len(rykzak_black_records)}")
    
    if not rykzak_black_records:
        print(f"\n❌ Артикул '{TARGET_ARTICLE}' не найден ни в одной из поставок")
        return
    
    # Группируем по складам
    warehouse_stats = defaultdict(lambda: {'quantity': 0, 'supplies': set()})
    total_quantity = 0
    supply_ids_with_article = set()
    
    for record in rykzak_black_records:
        warehouse_name = record.get('warehouseName', 'Неизвестный склад')
        quantity = record.get('quantity', 0)
        income_id = record.get('incomeId')
        
        warehouse_stats[warehouse_name]['quantity'] += quantity
        warehouse_stats[warehouse_name]['supplies'].add(income_id)
        total_quantity += quantity
        supply_ids_with_article.add(income_id)
    
    # Выводим результаты
    print("\n" + "=" * 80)
    print("📊 РЕЗУЛЬТАТЫ ПОИСКА")
    print("=" * 80)
    print(f"\n✅ Всего поставок с артикулом '{TARGET_ARTICLE}': {len(supply_ids_with_article)}")
    print(f"📦 Общее количество поставленного товара: {total_quantity} шт.")
    print(f"\n📋 Список поставок с артикулом '{TARGET_ARTICLE}':")
    for supply_id in sorted(supply_ids_with_article):
        print(f"   - Поставка {supply_id}")
    
    print(f"\n🏭 РАЗБИВКА ПО СКЛАДАМ:")
    print("-" * 80)
    for warehouse_name in sorted(warehouse_stats.keys()):
        stats = warehouse_stats[warehouse_name]
        print(f"\n📦 Склад: {warehouse_name}")
        print(f"   Количество: {stats['quantity']} шт.")
        print(f"   Поставок: {len(stats['supplies'])}")
        print(f"   ID поставок: {', '.join(map(str, sorted(stats['supplies'])))}")
    
    # Сохраняем детальные результаты
    output_file = "rykzak_black_results.json"
    results = {
        'target_article': TARGET_ARTICLE,
        'total_quantity': total_quantity,
        'total_supplies': len(supply_ids_with_article),
        'supply_ids': sorted(list(supply_ids_with_article)),
        'warehouse_breakdown': {
            warehouse: {
                'quantity': stats['quantity'],
                'supply_ids': sorted(list(stats['supplies']))
            }
            for warehouse, stats in warehouse_stats.items()
        },
        'detailed_records': rykzak_black_records
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\n💾 Детальные результаты сохранены в {output_file}")
    
    print("\n" + "=" * 80)
    print("✅ Обработка завершена")
    print("=" * 80)


if __name__ == "__main__":
    main()

