#!/usr/bin/env python3
"""
Main скрипт для загрузки CR Daily Stats из WB API в Supabase.

Полный цикл:
1. Запрос данных из API
2. Валидация структуры ответа
3. Трансформация данных (сегодня + вчера)
4. Обогащение product_id из таблицы products
5. Фильтрация записей без product_id
6. Upsert в Supabase
"""

import sys
from datetime import datetime, timedelta, date
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wb_api.cr_daily_stats.cr_daily_stats import fetch_cr_daily_stats
import wb_api.cr_daily_stats.cr_daily_stats as cr_api
from excel_actions.cr_daily_stats_ea.structure_validator import validate_cr_daily_stats_structure
from excel_actions.cr_daily_stats_ea.transform import extract_cr_stats_for_supabase
from excel_actions.cr_daily_stats_ea.supabase_writer import (
    enrich_with_product_ids,
    upsert_records
)
from excel_actions.cr_daily_stats_ea.data_validator import validate_inserted_data

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


def main():
    """Main entry point"""
    print("=" * 60)
    print("CR Daily Stats → Supabase")
    print("=" * 60)
    
    # ------------------------------------------------------------------
    # MANUAL DATE OVERRIDE — EDIT HERE IF NEEDED (как в paid_storage)
    # Пример:
    # manual_begin = date(2025, 10, 19)
    # manual_end = None  # один день
    manual_begin: date | None = None # ← установите date(...) чтобы переопределить
    manual_end: date | None = None    # ← установите date(...) чтобы переопределить
    # ------------------------------------------------------------------

    # 0. Период: ручной блок или дефолт (сегодня 00:00 → сейчас)
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    tz = ZoneInfo("Europe/Moscow")
    now_local = datetime.now(tz).replace(microsecond=0)

    if manual_begin is None and manual_end is None:
        # дефолт: сегодня 00:00 → сейчас
        begin_dt = now_local.replace(hour=0, minute=0, second=0)
        end_dt = now_local
        print(f"📅 Период (дефолт): {begin_dt.date()} → {end_dt.date()} (today)")
    else:
        # если задана только одна дата — считаем одним днём
        if manual_begin is None:
            manual_begin = manual_end
        if manual_end is None:
            manual_end = manual_begin
        begin_dt = datetime(manual_begin.year, manual_begin.month, manual_begin.day, 0, 0, 0, tzinfo=tz)
        end_dt = datetime(manual_end.year, manual_end.month, manual_end.day, 23, 59, 59, tzinfo=tz)
        print(f"📅 Период (ручной): {manual_begin} → {manual_end}")

    cr_api.payload["period"]["begin"] = cr_api.to_api_datetime(begin_dt)
    cr_api.payload["period"]["end"] = cr_api.to_api_datetime(end_dt)

    # 1. Запрос API
    print("\n📡 Шаг 1: Запрос данных из API")
    api_response = fetch_cr_daily_stats()
    cards_count = len(api_response.get('data', {}).get('cards', []))
    print(f"✅ Получено карточек: {cards_count}")
    
    if cards_count == 0:
        print("⚠️  Нет данных для обработки")
        return
    
    # 2. Валидация
    print("\n🔍 Шаг 2: Валидация структуры")
    try:
        validate_cr_daily_stats_structure(api_response)
    except ValueError as e:
        print(f"❌ Ошибка валидации: {e}")
        sys.exit(1)
    
    # 3. Трансформация
    print("\n🔄 Шаг 3: Трансформация данных")
    records_today, records_yesterday = extract_cr_stats_for_supabase(api_response)
    print(f"✅ Подготовлено записей за сегодня: {len(records_today)}")
    print(f"✅ Подготовлено записей за вчера: {len(records_yesterday)}")
    
    # 4. Подключение к Supabase
    print("\n🔌 Шаг 4: Подключение к Supabase")
    supabase = get_supabase_client()
    print(f"✅ Подключено к: {api_keys.SUPABASE_URL}")
    
    # 5. Обогащение product_id
    print("\n🔄 Шаг 5: Обогащение product_id")
    all_records = records_today + records_yesterday
    enriched_records = enrich_with_product_ids(all_records, supabase)
    
    if not enriched_records:
        print("⚠️  Нет записей для загрузки в БД (все артикулы отсутствуют в products)")
        return
    
    # Разделяем обратно на today и yesterday
    enriched_today = [r for r in enriched_records if r in records_today]
    enriched_yesterday = [r for r in enriched_records if r in records_yesterday]
    
    # 6. Upsert в БД
    print("\n💾 Шаг 6: Загрузка в Supabase")
    
    # Upsert записей за сегодня (с stocks)
    count_today = upsert_records(enriched_today, supabase, "сегодня")
    
    # Upsert записей за вчера (без stocks)
    count_yesterday = upsert_records(enriched_yesterday, supabase, "вчера")
    
    # 7. Валидация записанных данных
    print("\n🔍 Шаг 7: Валидация записанных данных")
    # Определяем даты из самих записей (надёжно при ручном периоде)
    def _pick_date(records):
        return str(records[0]["date_of_period"]) if records else None

    date_today = _pick_date(enriched_today)
    date_yesterday = _pick_date(enriched_yesterday)

    if date_today:
        validate_inserted_data(enriched_today, date_today, supabase, "сегодня")
    if date_yesterday:
        validate_inserted_data(enriched_yesterday, date_yesterday, supabase, "вчера")
    
    # Итоги
    print("\n" + "=" * 60)
    print("🎉 ГОТОВО!")
    print(f"📊 Обработано уникальных артикулов: {len(set(r['nm_id'] for r in enriched_records))}")
    print(f"📅 Записей за сегодня: {count_today}")
    print(f"📅 Записей за вчера: {count_yesterday}")
    print(f"💾 Всего записей в БД: {count_today + count_yesterday}")
    print("=" * 60)


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

