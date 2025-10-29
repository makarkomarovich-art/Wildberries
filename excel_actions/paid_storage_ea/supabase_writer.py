from __future__ import annotations

from decimal import Decimal
from typing import Dict, Iterable, List, Set, Tuple, Any
from supabase import Client


def build_products_map(supabase: Client) -> Dict[int, str]:
    resp = supabase.table('products').select('nm_id, id').execute()
    return {row['nm_id']: row['id'] for row in resp.data}


def _convert_decimal_to_float(data: Any) -> Any:
    """Recursively convert Decimal to float for JSON serialization"""
    if isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, dict):
        return {k: _convert_decimal_to_float(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_convert_decimal_to_float(item) for item in data]
    else:
        return data


def split_known_unknown_nmids(
    aggregated: List[dict],
    products_map: Dict[int, str]
) -> Tuple[List[dict], Set[int]]:
    known: List[dict] = []
    missing: Set[int] = set()
    for row in aggregated:
        nm_id = row['nm_id']
        product_id = products_map.get(nm_id)
        if product_id:
            out = dict(row)
            out['product_id'] = product_id
            # Convert Decimal to float for Supabase
            out = _convert_decimal_to_float(out)
            known.append(out)
        else:
            missing.add(nm_id)
    return known, missing


def upsert_paid_storage(
    rows: List[dict],
    supabase: Client
) -> Tuple[int, List[dict]]:
    """
    Upsert into paid_storage on conflict (nm_id,date).
    Returns (processed_count, updated_details[]), where updated_details items have
    {nm_id, date, old_price, new_price} for changed rows.
    """
    if not rows:
        return 0, []

    # Load existing for comparison
    keys = [(r['nm_id'], r['date']) for r in rows]
    # Reduce to unique keys for query
    unique_keys = sorted(set(keys))

    # Fetch existing rows for these keys
    existing_map: Dict[Tuple[int, str], dict] = {}
    # Query in chunks to avoid URL length issues
    chunk_size = 200
    for i in range(0, len(unique_keys), chunk_size):
        part = unique_keys[i:i+chunk_size]
        # Build OR filters using RPC-like pattern is not available; do simple range by date then filter client-side
        # Fetch by minimal constraints: between min and max date
        min_date = min(d for _, d in part)
        max_date = max(d for _, d in part)
        resp = supabase.table('paid_storage')\
            .select('*')\
            .gte('date', min_date)\
            .lte('date', max_date)\
            .execute()
        for row in resp.data:
            k = (row['nm_id'], row['date'])
            if k in part:
                existing_map[k] = row

    # Decide what to write: only new rows or rows where warehouse_price changed
    to_write: List[dict] = []
    updated_details: List[dict] = []
    for r in rows:
        k = (r['nm_id'], r['date'])
        if k not in existing_map:
            to_write.append(r)
            continue
        old = existing_map[k]
        old_price = old.get('warehouse_price')
        new_price = r['warehouse_price']
        if str(old_price) != str(new_price):
            to_write.append(r)
            updated_details.append({
                'nm_id': r['nm_id'],
                'date': r['date'],
                'old_price': old_price,
                'new_price': new_price,
            })

    if not to_write:
        return 0, []

    # Upsert only the necessary rows
    response = supabase.table('paid_storage').upsert(
        to_write,
        on_conflict='nm_id,date'
    ).execute()

    processed = len(response.data) if response.data else len(to_write)
    return processed, updated_details


