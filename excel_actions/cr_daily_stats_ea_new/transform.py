"""
Transform for CR History → records for cr_daily_stats_new.
"""
from __future__ import annotations
from typing import Any, Dict, List


def _all_zero_day(day: Dict[str, Any]) -> bool:
    # Filter zero rows by these fields
    keys = ("openCount", "cartCount", "orderCount", "orderSum", "buyoutCount", "buyoutSum")
    for k in keys:
        v = day.get(k) or 0
        if isinstance(v, (int, float)) and v != 0:
            return False
    return True


def _calc_price(total: Any, count: Any) -> float | None:
    try:
        total_v = float(total)
        count_v = int(count)
        if count_v > 0:
            return round(total_v / count_v, 2)
    except Exception:
        pass
    return None


def history_to_records(api_response: dict) -> List[dict]:
    """
    Build flat list of records for DB:
    - Skip products with empty title
    - For each day in history: skip if all zero by rule
    """
    items = api_response.get("data", [])
    out: List[dict] = []
    for it in items:
        product = it.get("product", {}) or {}
        title = product.get("title", "")
        if not title:
            # skip entirely
            continue
        nm_id = product.get("nmId")
        vendor_code = product.get("vendorCode") or ""
        history = it.get("history", []) or []
        if nm_id is None:
            continue
        try:
            nm_id = int(nm_id)
        except Exception:
            continue
        for day in history:
            if not isinstance(day, dict):
                continue
            if _all_zero_day(day):
                continue
            date_str = day.get("date")
            record = {
                "nm_id": nm_id,
                "vendor_code": vendor_code,
                "date_of_period": date_str,
                "open_card_count": day.get("openCount"),
                "add_to_cart_count": day.get("cartCount"),
                "orders_count": day.get("orderCount"),
                "orders_sum_rub": day.get("orderSum"),
                "buyouts_count": day.get("buyoutCount"),
                "buyouts_sum_rub": day.get("buyoutSum"),
                "add_to_cart_percent": day.get("addToCartConversion"),
                "cart_to_order_percent": day.get("cartToOrderConversion"),
                "order_price": _calc_price(day.get("orderSum"), day.get("orderCount")),
                "buyout_price": _calc_price(day.get("buyoutSum"), day.get("buyoutCount")),
            }
            out.append(record)
    return out


