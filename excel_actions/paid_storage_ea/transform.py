from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Tuple


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def aggregate_paid_storage(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregate warehousePrice by (date, nm_id). Also collect giId values into array.
    Output rows:
      - date (YYYY-MM-DD string)
      - nm_id (int)
      - vendor_code (str)
      - warehouse_price (Decimal rounded to 2)
      - gi_ids (list[int]) optional
    """
    bucket: Dict[Tuple[str, int], Dict[str, Any]] = {}

    for row in records:
        date_str = row.get("date")
        nm_id = row.get("nmId")
        vendor_code = row.get("vendorCode")
        price = row.get("warehousePrice", 0)
        gi_id = row.get("giId")

        if not isinstance(date_str, str) or not isinstance(nm_id, int):
            continue

        key = (date_str[:10], nm_id)
        acc = bucket.get(key)
        if acc is None:
            acc = {
                "date": key[0],
                "nm_id": nm_id,
                "vendor_code": vendor_code or "",
                "warehouse_price": Decimal("0"),
                "gi_ids": [],
            }
            bucket[key] = acc

        acc["warehouse_price"] += _to_decimal(price)
        if isinstance(gi_id, int):
            acc["gi_ids"].append(gi_id)

    # finalize rounding and deduplicate gi_ids
    result: List[Dict[str, Any]] = []
    for acc in bucket.values():
        acc["warehouse_price"] = acc["warehouse_price"].quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if acc["gi_ids"]:
            acc["gi_ids"] = sorted(list({g for g in acc["gi_ids"] if isinstance(g, int)}))
        else:
            acc["gi_ids"] = None
        result.append(acc)

    # sort by date then nm_id for stable output
    result.sort(key=lambda r: (r["date"], r["nm_id"]))
    return result


def summarize_by_day(aggregated: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build per-day summary: {date, unique_nm_count, total_warehouse_price}
    """
    by_day: Dict[str, Dict[str, Any]] = {}
    for row in aggregated:
        d = row["date"]
        if d not in by_day:
            by_day[d] = {
                "date": d,
                "unique_nm_ids": set(),
                "total": Decimal("0"),
            }
        by_day[d]["unique_nm_ids"].add(row["nm_id"])
        by_day[d]["total"] += _to_decimal(row["warehouse_price"])

    out: List[Dict[str, Any]] = []
    for d, agg in sorted(by_day.items()):
        out.append(
            {
                "date": d,
                "unique_nm_count": len(agg["unique_nm_ids"]),
                "total_warehouse_price": agg["total"].quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            }
        )
    return out


