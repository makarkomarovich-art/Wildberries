#!/usr/bin/env python3
"""
Main скрипт для загрузки себестоимости из Google Sheets в Supabase.

Полный цикл:
1. Чтение данных из Google Sheets (лист "Поартикульно")
2. Поиск столбцов "Артикул" и "Себес"/"Себестоимость"
3. Сверка артикулов с таблицей products
4. Логирование отсутствующих артикулов
5. Проверка на дубликаты (nm_id + date)
6. Запись в таблицу cost_price
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from supabase import create_client, Client
import api_keys


def get_supabase_client() -> Client:
    """Создает и возвращает клиент Supabase"""
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    
    if not url or not key:
        raise RuntimeError(
            "Supabase credentials not configured. "
            "Check SUPABASE_URL and SUPABASE_KEY in api_keys.py"
        )
    
    return create_client(url, key)


def get_google_sheets_service():
    """Получает сервис Google Sheets API."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    if api_keys.GOOGLE_CREDENTIALS_INFO:
        credentials = Credentials.from_service_account_info(
            api_keys.GOOGLE_CREDENTIALS_INFO, 
            scopes=scopes
        )
    else:
        credentials = Credentials.from_service_account_file(
            api_keys.GOOGLE_CREDENTIALS_FILE, 
            scopes=scopes
        )
    
    return build('sheets', 'v4', credentials=credentials)


def normalize_header(header: str) -> str:
    """Нормализует заголовок для сравнения."""
    return " ".join(str(header).strip().lower().split())


def find_column_indices(
    service,
    spreadsheet_id: str,
    sheet_name: str
) -> Tuple[Optional[int], Optional[int]]:
    """
    Находит индексы столбцов "Артикул" и "Себес"/"Себестоимость".
    
    Returns:
        Tuple[article_col_index, cost_col_index] (0-based индексы или None)
    """
    # Читаем первую строку (заголовки)
    range_name = f"{sheet_name}!1:1"
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name
    ).execute()
    
    headers = result.get('values', [[]])
    headers = headers[0] if headers else []
    
    article_col = None
    cost_col = None
    
    # Ищем столбцы
    for idx, header in enumerate(headers):
        normalized = normalize_header(header)
        
        # Ищем столбец "Артикул"
        if normalized in ['артикул']:
            article_col = idx
        
        # Ищем столбцы "Себес" или "Себестоимость"
        if normalized in ['себес', 'себестоимость']:
            cost_col = idx
    
    return article_col, cost_col


def _col_index_to_letter(index_0based: int) -> str:
    """Конвертирует 0-based индекс столбца в букву (A, B, C, ...)."""
    result = ""
    idx = index_0based
    while idx >= 0:
        result = chr(65 + (idx % 26)) + result
        idx = idx // 26 - 1
        if idx < 0:
            break
    return result


def read_cost_price_data(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    article_col: int,
    cost_col: int
) -> Dict[int, float]:
    """
    Читает данные из Google Sheets: nm_id -> cost_price.
    
    Returns:
        Dict: {nm_id: cost_price, ...}
    """
    # Определяем буквы столбцов
    article_letter = _col_index_to_letter(article_col)
    cost_letter = _col_index_to_letter(cost_col)
    
    # Читаем данные из двух столбцов начиная со 2 строки
    # Формат: "{sheet_name}!{col}2:{col}" - читает все строки начиная с 2
    article_range = f"{sheet_name}!{article_letter}2:{article_letter}"
    cost_range = f"{sheet_name}!{cost_letter}2:{cost_letter}"
    
    # Batch запрос для чтения обеих колонок
    result = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=[article_range, cost_range]
    ).execute()
    
    value_ranges = result.get('valueRanges', [])
    if len(value_ranges) != 2:
        raise ValueError("Не удалось прочитать данные из Google Sheets")
    
    article_values = value_ranges[0].get('values', [])
    cost_values = value_ranges[1].get('values', [])
    
    cost_data = {}
    max_rows = max(len(article_values), len(cost_values))
    
    for row_idx in range(max_rows):
        try:
            # Читаем артикул (nm_id)
            if row_idx >= len(article_values) or not article_values[row_idx]:
                continue
            article_str = str(article_values[row_idx][0]).strip()
            if not article_str:
                continue
            
            nm_id = int(article_str)
            
            # Читаем себестоимость
            if row_idx >= len(cost_values) or not cost_values[row_idx]:
                continue
            cost_str = str(cost_values[row_idx][0]).strip()
            if not cost_str:
                continue
            
            # Обрабатываем различные форматы чисел
            cost_str = cost_str.replace(',', '.').replace(' ', '').replace('\xa0', '')
            cost_price = float(cost_str)
            
            if nm_id and cost_price > 0:
                cost_data[nm_id] = cost_price
                
        except (ValueError, IndexError, TypeError) as e:
            # Пропускаем некорректные строки
            continue
    
    return cost_data


def get_products_map(supabase: Client) -> Dict[int, Dict[str, Any]]:
    """
    Получает карту products: nm_id -> {id, nm_id, vendor_code}.
    
    Returns:
        Dict: {nm_id: {id: uuid, nm_id: int, vendor_code: str}, ...}
    """
    print("🔄 Загрузка данных из таблицы products...")
    
    response = supabase.table('products').select('id, nm_id, vendor_code').execute()
    
    products_map = {}
    for product in response.data:
        nm_id = product.get('nm_id')
        if nm_id:
            products_map[nm_id] = {
                'id': product.get('id'),
                'nm_id': nm_id,
                'vendor_code': product.get('vendor_code', '')
            }
    
    print(f"✅ Загружено products: {len(products_map)}")
    return products_map


def check_existing_records(
    supabase: Client,
    nm_ids: Set[int],
    current_date: date
) -> Set[Tuple[int, date]]:
    """
    Проверяет существующие записи для данных nm_id и даты.
    
    Returns:
        Set: {(nm_id, date), ...} - существующие пары
    """
    if not nm_ids:
        return set()
    
    date_str = current_date.isoformat()
    
    response = supabase.table('cost_price')\
        .select('nm_id, date')\
        .in_('nm_id', list(nm_ids))\
        .eq('date', date_str)\
        .execute()
    
    existing = set()
    for row in response.data:
        existing.add((row['nm_id'], row['date']))
    
    return existing


def main() -> None:
    """Main entry point"""
    print("=" * 70)
    print("💰 Cost Price → Supabase (из Google Sheets)")
    print("=" * 70)
    
    # Настройки
    SHEET_NAME = "Поартикульно"
    SPREADSHEET_ID = api_keys.GOOGLE_SHEET_ID_COST_PRICE
    CURRENT_DATE = date.today()
    
    print(f"📊 Google Sheet ID: {SPREADSHEET_ID}")
    print(f"📋 Лист: {SHEET_NAME}")
    print(f"📅 Дата: {CURRENT_DATE}")
    print()
    
    # 1. Подключение к Google Sheets
    print("🔌 Шаг 1: Подключение к Google Sheets")
    try:
        service = get_google_sheets_service()
        print("✅ Подключено к Google Sheets API")
    except Exception as e:
        print(f"❌ Ошибка подключения к Google Sheets: {e}")
        sys.exit(1)
    
    # 2. Поиск столбцов
    print("\n🔍 Шаг 2: Поиск столбцов 'Артикул' и 'Себес'/'Себестоимость'")
    try:
        article_col, cost_col = find_column_indices(service, SPREADSHEET_ID, SHEET_NAME)
        
        if article_col is None:
            print("❌ Столбец 'Артикул' не найден")
            sys.exit(1)
        
        if cost_col is None:
            print("❌ Столбец 'Себес'/'Себестоимость' не найден")
            sys.exit(1)
        
        print(f"✅ Найден столбец 'Артикул': индекс {article_col}")
        print(f"✅ Найден столбец 'Себес'/'Себестоимость': индекс {cost_col}")
    except Exception as e:
        print(f"❌ Ошибка при поиске столбцов: {e}")
        sys.exit(1)
    
    # 3. Чтение данных из Google Sheets
    print("\n📖 Шаг 3: Чтение данных из Google Sheets")
    try:
        cost_data = read_cost_price_data(
            service, 
            SPREADSHEET_ID, 
            SHEET_NAME, 
            article_col, 
            cost_col
        )
        print(f"✅ Прочитано пар (nm_id, cost_price): {len(cost_data)}")
        
        # Выводим первые 5 примеров
        print("\n📝 Первые 5 примеров nm_id → себестоимость:")
        for idx, (nm_id, cost) in enumerate(list(cost_data.items())[:5], 1):
            print(f"   {idx}. nm_id={nm_id}, себестоимость={cost}")
    except Exception as e:
        print(f"❌ Ошибка при чтении данных: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    if not cost_data:
        print("⚠️  Нет данных для обработки")
        return
    
    # 4. Подключение к Supabase
    print("\n🔌 Шаг 4: Подключение к Supabase")
    try:
        supabase = get_supabase_client()
        print(f"✅ Подключено к: {api_keys.SUPABASE_URL}")
    except Exception as e:
        print(f"❌ Ошибка подключения к Supabase: {e}")
        sys.exit(1)
    
    # 5. Загрузка products
    print("\n📦 Шаг 5: Загрузка данных из таблицы products")
    products_map = get_products_map(supabase)
    
    # 6. Анализ совпадений
    print("\n🔍 Шаг 6: Анализ совпадений артикулов")
    
    google_nm_ids = set(cost_data.keys())
    products_nm_ids = set(products_map.keys())
    
    # Артикулы в Google, но не в products
    only_in_google = google_nm_ids - products_nm_ids
    if only_in_google:
        print(f"\n⚠️  Артикулы в Google таблице, но отсутствующие в products ({len(only_in_google)}):")
        sorted_missing = sorted(only_in_google)
        print(f"   Примеры: {', '.join(str(x) for x in sorted_missing[:10])}")
        if len(sorted_missing) > 10:
            print(f"   ... и еще {len(sorted_missing) - 10}")
    
    # Артикулы в products, но не в Google
    only_in_products = products_nm_ids - google_nm_ids
    if only_in_products:
        print(f"\n⚠️  Артикулы в products, но отсутствующие в Google таблице ({len(only_in_products)}):")
        sorted_missing = sorted(only_in_products)
        print(f"   Примеры: {', '.join(str(x) for x in sorted_missing[:10])}")
        if len(sorted_missing) > 10:
            print(f"   ... и еще {len(sorted_missing) - 10}")
    
    # Артикулы, которые есть в обоих местах
    matching_nm_ids = google_nm_ids & products_nm_ids
    print(f"\n✅ Найдено совпадений: {len(matching_nm_ids)} артикулов")
    
    if not matching_nm_ids:
        print("⚠️  Нет совпадений для обработки")
        return
    
    # 7. Проверка на дубликаты
    print(f"\n🔍 Шаг 7: Проверка существующих записей для даты {CURRENT_DATE}")
    existing_records = check_existing_records(supabase, matching_nm_ids, CURRENT_DATE)
    
    if existing_records:
        existing_count = len(existing_records)
        existing_nm_ids = {nm_id for nm_id, _ in existing_records}
        print(f"⚠️  Найдено существующих записей: {existing_count}")
        print(f"   Примеры nm_id: {', '.join(str(x) for x in sorted(existing_nm_ids)[:10])}")
        
        # Исключаем существующие из обработки
        matching_nm_ids = matching_nm_ids - existing_nm_ids
        print(f"✅ К обработке осталось: {len(matching_nm_ids)} артикулов")
    else:
        print(f"✅ Дубликатов не найдено, обрабатываем все {len(matching_nm_ids)} артикулов")
    
    if not matching_nm_ids:
        print("⚠️  Все артикулы уже существуют в БД для этой даты")
        return
    
    # 8. Подготовка данных для вставки
    print(f"\n📝 Шаг 8: Подготовка данных для вставки")
    records_to_insert = []
    
    for nm_id in matching_nm_ids:
        product = products_map[nm_id]
        cost_price = cost_data[nm_id]
        
        record = {
            'product_id': product['id'],
            'nm_id': nm_id,
            'vendor_code': product['vendor_code'],
            'date': CURRENT_DATE.isoformat(),
            'cost_price': cost_price
        }
        records_to_insert.append(record)
    
    print(f"✅ Подготовлено записей: {len(records_to_insert)}")
    
    # 9. Вставка в БД
    print(f"\n💾 Шаг 9: Запись в БД")
    inserted_count = 0
    
    try:
        # Используем upsert для избежания дубликатов
        for record in records_to_insert:
            try:
                supabase.table('cost_price').upsert(
                    record,
                    on_conflict='nm_id,date'
                ).execute()
                inserted_count += 1
            except Exception as e:
                print(f"⚠️  Ошибка при вставке nm_id={record['nm_id']}: {e}")
                continue
        
        print(f"✅ Добавлено записей: {inserted_count}")
        print(f"📊 Всего найдено пар (nm_id, cost_price): {len(cost_data)}")
        print(f"📊 Внесено в БД: {inserted_count}")
        
    except Exception as e:
        print(f"❌ Ошибка при записи в БД: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("🎉 ГОТОВО!")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

