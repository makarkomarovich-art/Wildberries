#!/usr/bin/env python3
"""
CR Daily Stats (history) client:
POST /api/analytics/v3/sales-funnel/products/history

- Accepts up to 20 nmIds per request
- Aggregation level: day
"""
from __future__ import annotations

import os
import json
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests
import sys
from pathlib import Path

# Ensure project root on sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_keys

BASE_URL = os.getenv("WB_BASE_URL", "https://seller-analytics-api.wildberries.ru")
ENDPOINT_PATH = "/api/analytics/v3/sales-funnel/products/history"
URL = BASE_URL.rstrip("/") + ENDPOINT_PATH

TOKEN = getattr(api_keys, "WB_API_TOKEN", None)
USER_AGENT = getattr(api_keys, "USER_AGENT", "CR_history/1.0")
COOKIES = getattr(api_keys, "COOKIES", None)

if not TOKEN:
    raise RuntimeError("WB_API_TOKEN not found in api_keys.py")

HEADERS = {
    "Authorization": TOKEN,
    "Content-Type": "application/json",
    "User-Agent": USER_AGENT,
}
if COOKIES:
    HEADERS["Cookie"] = COOKIES

TZ = ZoneInfo("Europe/Moscow")


def _to_date_string(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def post_with_retries(url: str, json_body: Dict[str, Any], retries: int = 3, backoff: int = 2, timeout: int = 60):
    attempt = 0
    while True:
        try:
            resp = requests.post(url, headers=HEADERS, json=json_body, timeout=timeout)
        except requests.RequestException:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(backoff ** attempt)
            continue

        if resp.status_code == 200:
            return resp
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
            wait = backoff ** (attempt + 1)
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    wait = int(ra)
                except Exception:
                    pass
            time.sleep(wait)
            attempt += 1
            continue
        # Non-retriable or exhausted
        return resp


def build_body(start_date: date, end_date: date, nm_ids: List[int]) -> Dict[str, Any]:
    return {
        "selectedPeriod": {"start": _to_date_string(start_date), "end": _to_date_string(end_date)},
        "nmIds": nm_ids,
        "skipDeletedNm": True,
        "aggregationLevel": "day",
    }


def fetch_history_chunk(start_date: date, end_date: date, nm_ids: List[int]) -> Dict[str, Any]:
    """Fetch history for a chunk (<=20) of nmIds. Returns raw JSON dict."""
    body = build_body(start_date, end_date, nm_ids)
    resp = post_with_retries(URL, body)
    if resp.status_code != 200:
        try:
            print(resp.text[:500])
        except Exception:
            pass
        resp.raise_for_status()
    return resp.json()


def chunked(seq: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def save_response_json(resp_json: Any, filename: str | None = None) -> str:
    """Save response JSON into this module directory."""
    if not filename:
        ts = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        filename = f"cr_history_response_{ts}.json"
    out_path = SCRIPT_DIR / filename
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(resp_json, f, ensure_ascii=False, indent=2)
    return str(out_path)


def fetch_history_all(start_date: date, end_date: date, nm_ids: List[int], sleep_seconds: float = 20.0) -> List[Dict[str, Any]]:
    """
    Fetch history for all nm_ids, chunked by 20 with delays for rate limit.
    Returns a flat list (concatenated results arrays).
    """
    all_items: List[Dict[str, Any]] = []
    print(f"🔄 История CR: {URL}")
    print(f"📅 Период: {start_date} → {end_date}")
    print(f"🧩 Всего nmIds: {len(nm_ids)}; чанки по 20")
    for idx, group in enumerate(chunked(nm_ids, 20), start=1):
        print(f"   ▸ Чанк {idx}: nmIds={len(group)}")
        js = fetch_history_chunk(start_date, end_date, group)
        # API may return a top-level array or an object with 'data'
        if isinstance(js, list):
            data_items = js
        elif isinstance(js, dict):
            data_items = js.get("data", [])
        else:
            data_items = []
        print(f"     + Получено: {len(data_items)}")
        all_items.extend(data_items)
        if len(group) == 20:
            time.sleep(sleep_seconds)
    print(f"✅ Итого получено записей (products): {len(all_items)}")
    return all_items


def _load_all_nm_ids_from_supabase() -> List[int]:
    """Utility for standalone mode: load all nm_id from products table."""
    from supabase import create_client
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    sb = create_client(url, key)
    resp = sb.table("products").select("nm_id").execute()
    nm_ids: List[int] = []
    for row in resp.data or []:
        nm = row.get("nm_id")
        if isinstance(nm, int):
            nm_ids.append(nm)
    return nm_ids


def main():
    """Standalone: fetch all products' history for last 7 days and save JSON."""
    today = datetime.now(TZ).date()
    start = today - timedelta(days=6)
    end = today
    nm_ids = _load_all_nm_ids_from_supabase()
    if not nm_ids:
        print("⚠️  В таблице products нет nm_id")
        return
    data = fetch_history_all(start, end, nm_ids, sleep_seconds=20.0)
    out = save_response_json({"data": data})
    print(f"💾 JSON сохранён: {out}")


if __name__ == "__main__":
    main()


