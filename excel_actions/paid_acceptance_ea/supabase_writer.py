from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Set, Tuple
from supabase import Client


def build_products_maps(supabase: Client) -> Tuple[Dict[int, str], Dict[int, str]]:
    """Return two maps: nm_id->product_id, nm_id->vendor_code"""
    resp = supabase.table('products').select('nm_id, id, vendor_code').execute()
    nm_to_product: Dict[int, str] = {}
    nm_to_vendor: Dict[int, str] = {}
    for row in resp.data:
        nm_to_product[row['nm_id']] = row['id']
        nm_to_vendor[row['nm_id']] = row['vendor_code']
    return nm_to_product, nm_to_vendor


def _convert_decimal_to_float(data: Any) -> Any:
    if isinstance(data, Decimal):
        return float(data)
    if isinstance(data, dict):
        return {k: _convert_decimal_to_float(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_convert_decimal_to_float(x) for x in data]
    return data


def split_known_unknown_nmids(
    aggregated: List[dict],
    products_map: Dict[int, str],
    vendor_map: Dict[int, str]
) -> Tuple[List[dict], Set[int]]:
    known: List[dict] = []
    missing: Set[int] = set()
    for row in aggregated:
        nm_id = row['nm_id']
        product_id = products_map.get(nm_id)
        vendor_code = vendor_map.get(nm_id)
        if product_id and vendor_code:
            out = dict(row)
            out['product_id'] = product_id
            out['vendor_code'] = vendor_code
            out = _convert_decimal_to_float(out)
            known.append(out)
        else:
            missing.add(nm_id)
    return known, missing


def upsert_paid_acceptance(
    rows: List[dict],
    supabase: Client
) -> Tuple[int, List[dict]]:
    """
    Upsert into paid_acceptance on conflict (nm_id, shk_create_date).
    Returns (processed_count, updated_details[]), where updated_details items have
    {nm_id, shk_create_date, old_count, new_count, old_total, new_total} for changed rows.
    """
    if not rows:
        return 0, []

    keys = [(r['nm_id'], r['shk_create_date']) for r in rows]
    unique_keys = sorted(set(keys))

    existing_map: Dict[Tuple[int, str], dict] = {}
    chunk_size = 200
    for i in range(0, len(unique_keys), chunk_size):
        part = unique_keys[i:i+chunk_size]
        min_date = min(d for _, d in part)
        max_date = max(d for _, d in part)
        resp = supabase.table('paid_acceptance')\
            .select('*')\
            .gte('shk_create_date', min_date)\
            .lte('shk_create_date', max_date)\
            .execute()
        for row in resp.data:
            k = (row['nm_id'], row['shk_create_date'])
            if k in part:
                existing_map[k] = row

    to_write: List[dict] = []
    updated_details: List[dict] = []
    for r in rows:
        k = (r['nm_id'], r['shk_create_date'])
        if k not in existing_map:
            to_write.append(r)
            continue
        old = existing_map[k]
        old_count = old.get('count')
        old_total = old.get('total')
        new_count = r['count']
        new_total = r['total']
        if str(old_count) != str(new_count) or str(old_total) != str(new_total):
            to_write.append(r)
            updated_details.append({
                'nm_id': r['nm_id'],
                'shk_create_date': r['shk_create_date'],
                'old_count': old_count,
                'new_count': new_count,
                'old_total': old_total,
                'new_total': new_total,
            })

    if not to_write:
        return 0, []

    response = supabase.table('paid_acceptance').upsert(
        to_write,
        on_conflict='nm_id,shk_create_date'
    ).execute()

    processed = len(response.data) if response.data else len(to_write)
    return processed, updated_details


