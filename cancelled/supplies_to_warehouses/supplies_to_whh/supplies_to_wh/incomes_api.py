#!/usr/bin/env python3
"""
API-клиент для загрузки данных поставок из WB API /incomes.
Поддерживает пагинацию и автоматическую загрузку всех данных.
"""
import os
import json
import time
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests
import sys

# Ensure project root is on sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import API keys
import api_keys

# Config
BASE_URL = "https://statistics-api.wildberries.ru"
ENDPOINT_PATH = "/api/v1/supplier/incomes"
URL = BASE_URL + ENDPOINT_PATH

TOKEN = getattr(api_keys, "WB_API_TOKEN", None)
USER_AGENT = getattr(api_keys, "USER_AGENT", "Supplies_incomes/1.0")

if not TOKEN:
    raise RuntimeError("WB_API_TOKEN not found in api_keys.py")

HEADERS = {
    "Authorization": TOKEN,
    "Content-Type": "application/json",
    "User-Agent": USER_AGENT,
}

# Timezone
TZ_NAME = "Europe/Moscow"
tz = ZoneInfo(TZ_NAME)

# Rate limiting: 1 запрос в минуту
RATE_LIMIT_DELAY = 60  # секунды


def get_default_date_from():
    """Возвращает дату месяц назад по умолчанию"""
    now = datetime.now(tz)
    month_ago = now - timedelta(days=30)
    return month_ago.strftime("%Y-%m-%dT00:00:00")


def fetch_incomes_page(date_from: str) -> tuple[list, str | None]:
    """
    Загружает одну страницу данных поставок.
    
    Args:
        date_from: Дата начала в формате RFC3339
        
    Returns:
        tuple: (список записей, lastChangeDate последней записи или None)
    """
    params = {
        "dateFrom": date_from
    }
    
    print(f"🔄 Запрос поставок с {date_from}")
    print(f"🔑 API ключ: {TOKEN[:10]}...")
    
    try:
        resp = requests.get(URL, headers=HEADERS, params=params, timeout=30)
        print(f"📡 Статус ответа: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"❌ Ошибка: {resp.status_code}")
            print(f"Ответ: {resp.text}")
            resp.raise_for_status()
            
        data = resp.json()
        
        if not isinstance(data, list):
            raise ValueError(f"Ожидался массив, получен {type(data).__name__}")
        
        print(f"✅ Получено записей: {len(data)}")
        
        if data:
            last_change_date = data[-1].get('lastChangeDate')
            print(f"📅 Последняя дата изменения: {last_change_date}")
            return data, last_change_date
        else:
            print("📭 Нет данных")
            return [], None
            
    except requests.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")
        raise
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        raise


def fetch_incomes(date_from: str = None) -> list:
    """
    Загружает все данные поставок с пагинацией.
    
    Args:
        date_from: Дата начала в формате RFC3339. 
                   Если None, используется последний месяц.
                   
    Returns:
        list: Полный список всех записей поставок
    """
    if date_from is None:
        date_from = get_default_date_from()
        print(f"📅 Используется дата по умолчанию: {date_from}")
    
    all_records = []
    current_date = date_from
    page_count = 0
    
    while True:
        page_count += 1
        print(f"\n📄 Страница {page_count}")
        
        # Задержка между запросами (кроме первого)
        if page_count > 1:
            print(f"⏳ Ожидание {RATE_LIMIT_DELAY} секунд...")
            time.sleep(RATE_LIMIT_DELAY)
        
        try:
            records, last_change_date = fetch_incomes_page(current_date)
            all_records.extend(records)
            
            # Если получено меньше 100000 записей или нет данных - конец
            if len(records) < 100000 or not records:
                print(f"✅ Пагинация завершена. Всего записей: {len(all_records)}")
                break
                
            # Обновляем дату для следующего запроса
            if last_change_date:
                current_date = last_change_date
                print(f"🔄 Следующая страница с даты: {current_date}")
            else:
                print("❌ Нет lastChangeDate для продолжения пагинации")
                break
                
        except Exception as e:
            print(f"❌ Ошибка на странице {page_count}: {e}")
            raise
    
    return all_records


def save_response_json(data: list, filename: str = None) -> str:
    """Сохраняет ответ в JSON файл"""
    if not filename:
        timestamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
        filename = f"incomes_response_{timestamp}.json"
    
    out_path = os.path.join(SCRIPT_DIR, filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON сохранен: {out_path}")
    return out_path


def main():
    """Main entry point для standalone выполнения"""
    print("=" * 60)
    print("🔄 Загрузка данных поставок из WB API")
    print("=" * 60)
    
    try:
        # Загружаем данные за последний месяц
        data = fetch_incomes()
        
        if data:
            save_response_json(data)
            print(f"\n🎉 ГОТОВО! Загружено {len(data)} записей поставок")
            
            # Показываем статистику
            unique_incomes = len(set(record['incomeId'] for record in data))
            unique_nm_ids = len(set(record['nmId'] for record in data))
            statuses = {}
            for record in data:
                status = record.get('status', 'Unknown')
                statuses[status] = statuses.get(status, 0) + 1
            
            print(f"📊 Уникальных поставок: {unique_incomes}")
            print(f"📊 Уникальных товаров: {unique_nm_ids}")
            print(f"📊 Статусы: {statuses}")
        else:
            print("⚠️  Нет данных для загрузки")
            
    except KeyboardInterrupt:
        print("\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
