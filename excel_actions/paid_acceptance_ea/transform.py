from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, List, Tuple


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def aggregate_acceptance(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregate count and total by (shkCreateDate, nmID).
    Output rows:
      - shk_create_date (YYYY-MM-DD string)
      - nm_id (int)
      - count (int)
      - total (Decimal rounded to 2)
    """
    bucket: Dict[Tuple[str, int], Dict[str, Any]] = {}

    for row in records:
        date_str = row.get("shkCreateDate")
        nm_id = row.get("nmID")
        cnt = row.get("count", 0) or 0
        total_val = row.get("total", 0) or 0

        if not isinstance(date_str, str) or not isinstance(nm_id, int):
            continue

        key = (date_str[:10], nm_id)
        acc = bucket.get(key)
        if acc is None:
            acc = {
                "shk_create_date": key[0],
                "nm_id": nm_id,
                "count": 0,
                "total": Decimal("0"),
            }
            bucket[key] = acc

        acc["count"] += int(cnt)
        acc["total"] += _to_decimal(total_val)

    # finalize rounding
    result: List[Dict[str, Any]] = []
    for acc in bucket.values():
        acc["total"] = acc["total"].quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        result.append(acc)

    # stable sort
    result.sort(key=lambda r: (r["shk_create_date"], r["nm_id"]))
    return result


def per_article_logs(aggregated: List[Dict[str, Any]], nm_to_vendor: Dict[int, str]) -> List[str]:
    lines: List[str] = []
    for row in aggregated:
        nm_id = row["nm_id"]
        vendor = nm_to_vendor.get(nm_id, "")
        count = row["count"]
        total = row["total"]
        price_per_unit = (Decimal("0") if count == 0 else (Decimal(str(total)) / Decimal(count))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        lines.append(
            f"{vendor} - {row['shk_create_date']} - {count} - {total} - {price_per_unit}"
        )
    return lines


