#!/usr/bin/env python3
"""
Main for CR Daily Stats (history → cr_daily_stats_new).

Flow:
1) Period: manual override, default last 7 days (Europe/Moscow).
2) Load nm_ids from products; chunk by 20 with 20s delay.
3) Pre-validate API response.
4) Transform to records; filter zero rows; compute prices.
5) Enrich with product_id.
6) Delete-then-insert into cr_daily_stats_new.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List

# Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wb_api.cr_daily_stats_new.history_client import (
    fetch_history_all,
)
from excel_actions.cr_daily_stats_ea_new.structure_validator import validate_history_structure
from excel_actions.cr_daily_stats_ea_new.transform import history_to_records
from excel_actions.cr_daily_stats_ea_new.supabase_writer import (
    enrich_with_product_ids,
    delete_then_insert,
)
from excel_actions.cr_daily_stats_ea_new.data_validator import validate_inserted_data_new
from supabase import create_client, Client
import api_keys


def get_supabase_client() -> Client:
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    if not url or not key:
        raise RuntimeError("Supabase credentials not configured")
    return create_client(url, key)


def _load_all_nm_ids(sb: Client) -> List[int]:
    resp = sb.table("products").select("nm_id").execute()
    out: List[int] = []
    for row in resp.data or []:
        nm = row.get("nm_id")
        if isinstance(nm, int):
            out.append(nm)
    return out


def main():
    print("=" * 60)
    print("CR Daily Stats (history) → cr_daily_stats_new")
    print("=" * 60)

    # Manual period (default last 7 days)
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    tz = ZoneInfo("Europe/Moscow")
    now_local = datetime.now(tz).date()

    # EDIT if needed:
    manual_begin: date | None = None
    manual_end: date | None = None

    if manual_begin is None and manual_end is None:
        start = now_local - timedelta(days=6)
        end = now_local
        print(f"📅 Период (дефолт): {start} → {end} (last 7 days)")
    else:
        if manual_begin is None:
            manual_begin = manual_end
        if manual_end is None:
            manual_end = manual_begin
        # limit to last 7 days span
        span = (manual_end - manual_begin).days + 1
        if span > 7:
            manual_begin = manual_end - timedelta(days=6)
            print(f"⚠️  Период урезан до последних 7 дней: {manual_begin} → {manual_end}")
        start = manual_begin
        end = manual_end
        print(f"📅 Период (ручной): {start} → {end}")

    # 1. Supabase
    print("\n🔌 Шаг 1: Подключение к Supabase")
    sb = get_supabase_client()
    print(f"✅ Подключено к: {api_keys.SUPABASE_URL}")

    # 2. Load nm_ids
    print("\n📦 Шаг 2: Загрузка nm_id из products")
    nm_ids = _load_all_nm_ids(sb)
    print(f"✅ Загружено nm_id: {len(nm_ids)}")
    if not nm_ids:
        print("⚠️  Нет nm_id в products — выход")
        return

    # 3. API fetch (chunked)
    print("\n📡 Шаг 3: Запрос данных из API (history)")
    response = {"data": fetch_history_all(start, end, nm_ids, sleep_seconds=20.0)}
    print(f"✅ Элементов в data: {len(response.get('data', []))}")

    # 4. Pre-validation
    print("\n🔍 Шаг 4: Валидация структуры")
    if not validate_history_structure(response):
        print("❌ Структура ответа невалидна — останов")
        sys.exit(1)

    # 5. Transform
    print("\n🔄 Шаг 5: Трансформация данных")
    records = history_to_records(response)
    print(f"✅ Подготовлено записей: {len(records)}")
    if not records:
        print("⚠️  После фильтрации нет записей — выход")
        return

    # 6. Enrich product_id
    print("\n🔄 Шаг 6: Обогащение product_id")
    enriched = enrich_with_product_ids(records, sb)
    if not enriched:
        print("⚠️  Нет записей после обогащения — выход")
        return

    # 7. Delete → Insert
    print("\n💾 Шаг 7: Запись в cr_daily_stats_new (delete → insert)")
    deleted, inserted = delete_then_insert(enriched, sb, table_name="cr_daily_stats")

    # 8. Post-validation
    print("\n🔍 Шаг 8: Пост-валидация записанных данных")
    ok = validate_inserted_data_new(enriched, sb, verbose=False)
    if ok:
        print("✅ Валидация API→БД прошла успешно")
    else:
        print("⚠️  Валидация API→БД выявила расхождения")

    print("\n" + "=" * 60)
    print("🎉 ГОТОВО!")
    print(f"🗑️  Удалено: {deleted}")
    print(f"💾 Вставлено: {inserted}")
    print(f"📊 Уникальных nm_id: {len(set(r['nm_id'] for r in enriched))}")
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


