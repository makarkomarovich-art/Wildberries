"""
Post-валидация для cr_daily_stats_new:
Сверяет ожидаемые записи (из API после трансформации) с тем, что вставлено в БД.
"""
from __future__ import annotations
from typing import Dict, List
from collections import defaultdict
from supabase import Client


BUSINESS_FIELDS = [
    "nm_id",
    "vendor_code",
    "date_of_period",
    "open_card_count",
    "add_to_cart_count",
    "orders_count",
    "orders_sum_rub",
    "buyouts_count",
    "buyouts_sum_rub",
    "add_to_cart_percent",
    "cart_to_order_percent",
    "order_price",
    "buyout_price",
]


def _values_equal(a, b) -> bool:
    if a is None and b is None:
        return True
    if (a is None) != (b is None):
        return False
    # numeric tolerance
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) < 0.01
    return str(a) == str(b)


def validate_inserted_data_new(expected_records: List[dict], supabase: Client, verbose: bool = False) -> bool:
    """
    Проверяет все ожидаемые записи, сгруппированные по дате.
    Делает выборку из cr_daily_stats_new по каждой дате и сверяет поля.
    """
    if not expected_records:
        if verbose:
            print("⚠️  Нет данных для пост-валидации")
        return True

    by_date: Dict[str, List[dict]] = defaultdict(list)
    for r in expected_records:
        d = r.get("date_of_period")
        if d:
            by_date[str(d)].append(r)

    overall_ok = True
    for date_str, exp_rows in by_date.items():
        if verbose:
            print(f"🔎 Post-валидация за {date_str} (ожидали {len(exp_rows)} записей)")
        nm_ids = [r["nm_id"] for r in exp_rows]
        try:
            db_resp = (
                supabase.table("cr_daily_stats_new")
                .select("*")
                .eq("date_of_period", date_str)
                .in_("nm_id", nm_ids)
                .execute()
            )
            db_rows = db_resp.data or []
        except Exception as e:
            if verbose:
                print(f"⚠️  WARNING: Ошибка запроса к БД: {e}")
            overall_ok = False
            continue

        if len(db_rows) != len(exp_rows):
            if verbose:
                print(f"⚠️  WARNING: Кол-во не совпало: ожидали {len(exp_rows)}, в БД {len(db_rows)}")
            overall_ok = False

        db_map = {row["nm_id"]: row for row in db_rows}

        mismatches = []
        for exp in exp_rows:
            nm = exp["nm_id"]
            db = db_map.get(nm)
            if not db:
                mismatches.append(f"nm_id={nm} отсутствует в БД")
                continue
            for field in BUSINESS_FIELDS:
                ev = exp.get(field)
                av = db.get(field)
                if not _values_equal(ev, av):
                    mismatches.append(f"nm_id={nm} поле {field}: ожидали {ev}, БД {av}")

        if mismatches:
            overall_ok = False
            if verbose:
                print(f"⚠️  Найдено расхождений: {len(mismatches)} (покажем до 10)")
                for m in mismatches[:10]:
                    print(f"   ⚠️  {m}")
                if len(mismatches) > 10:
                    print(f"   ... и ещё {len(mismatches) - 10}")
        else:
            if verbose:
                print(f"✅ OK ({len(exp_rows)} записей)")

    return overall_ok


