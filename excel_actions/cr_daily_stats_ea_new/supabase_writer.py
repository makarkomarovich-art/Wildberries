"""
Writer for cr_daily_stats_new:
- Enrich product_id
- Delete-then-insert by (nm_id, date_of_period)
"""
from __future__ import annotations
from typing import Dict, List, Tuple, Set
from collections import defaultdict
from supabase import Client


def enrich_with_product_ids(records: List[dict], supabase: Client) -> List[dict]:
    if not records:
        return []
    print("🔄 Получение product_id из таблицы products...")
    resp = supabase.table("products").select("nm_id, id").execute()
    mapping = {row["nm_id"]: row["id"] for row in (resp.data or [])}
    print(f"✅ Загружено products: {len(mapping)}")
    out: List[dict] = []
    skipped = 0
    for r in records:
        nm = r["nm_id"]
        pid = mapping.get(nm)
        if not pid:
            print(f"⚠️  WARNING: nm_id={nm} не найден в products → пропуск")
            skipped += 1
            continue
        r["product_id"] = pid
        out.append(r)
    if skipped:
        print(f"⚠️  Пропущено (нет в products): {skipped}")
    print(f"✅ Обогащено записей: {len(out)}")
    return out


def delete_then_insert(records: List[dict], supabase: Client) -> Tuple[int, int]:
    """
    Deletes existing rows for incoming (nm_id, date_of_period) pairs, then inserts.
    Returns: (deleted_count, inserted_count)
    """
    if not records:
        print("⚠️  Нет записей для записи")
        return (0, 0)

    # Group dates by nm_id for composite-key delete
    nm_to_dates: Dict[int, Set[str]] = defaultdict(set)
    for r in records:
        nm_to_dates[r["nm_id"]].add(r["date_of_period"])

    total_deleted = 0
    print("🗑️  Удаление старых записей (по nm_id, date_of_period)...")
    for nm_id, dates in nm_to_dates.items():
        dates_list = list(dates)
        # Supabase python client: composite delete via eq + in_
        resp = supabase.table("cr_daily_stats_new").delete().eq("nm_id", nm_id).in_("date_of_period", dates_list).execute()
        deleted = getattr(resp, "count", None)
        # Some client versions don't return count; we log sizes heuristically
        total_deleted += deleted or 0
    print(f"✅ Удалено (по отчёту клиента): {total_deleted}")

    print("💾 Вставка новых записей...")
    # Insert in batches to avoid payload limits
    batch_size = 1000
    inserted_total = 0
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        resp = supabase.table("cr_daily_stats_new").insert(chunk).execute()
        inserted = len(resp.data) if resp.data is not None else len(chunk)
        inserted_total += inserted
        print(f"   ▸ Вставлено: {inserted}")
    print(f"✅ Всего вставлено: {inserted_total}")
    return (total_deleted, inserted_total)


