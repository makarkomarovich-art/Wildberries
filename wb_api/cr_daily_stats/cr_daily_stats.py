#!/usr/bin/env python3
"""
CR Daily Stats клиент (совместимый слой) поверх нового WB API:
POST /api/analytics/v3/sales-funnel/products

Задача: вернуть структуру в прежнем формате для остального кода:
{
  "data": {
    "cards": [
      {
        "nmID": int,
        "vendorCode": str,
        "statistics": {
          "selectedPeriod": { ...mapped metrics... },
          "previousPeriod": { ...mapped metrics... }
        },
        "stocks": { "stocksMp": int, "stocksWb": int }
      }
    ],
    "summary": {
      "selectedPeriod": { "begin": "YYYY-MM-DD 00:00:00", "end": "YYYY-MM-DD 23:59:59" },
      "previousPeriod": { "begin": "YYYY-MM-DD 00:00:00", "end": "YYYY-MM-DD 23:59:59" }
    }
  }
}
"""
import os
import json
import time
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests
import sys

# Ensure project root is on sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import API keys
import api_keys

# Config
BASE_URL = os.getenv("WB_BASE_URL", "https://seller-analytics-api.wildberries.ru")
ENDPOINT_PATH = "/api/analytics/v3/sales-funnel/products"
URL = BASE_URL.rstrip("/") + ENDPOINT_PATH

TOKEN = getattr(api_keys, "WB_API_TOKEN", None)
USER_AGENT = getattr(api_keys, "USER_AGENT", "CR_daily_stats/1.0")
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

# Timezone
TZ_NAME = "Europe/Moscow"
tz = ZoneInfo(TZ_NAME)

# Period: today 00:00:00 -> now
now = datetime.now(tz)
begin_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
end_dt = now.replace(microsecond=0)


def to_api_datetime(dt: datetime) -> str:
    """YYYY-MM-DD HH:MM:SS"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def to_api_date(d: datetime) -> str:
    """YYYY-MM-DD (date only)"""
    return d.strftime("%Y-%m-%d")


payload = {
    # совместимая обёртка; используем period.begin/end для вычисления selected/past
    "period": {
        "begin": to_api_datetime(begin_dt),
        "end": to_api_datetime(end_dt),
    },
    # фильтры (оставим пустыми, чтобы получить все товары)
    "nmIds": [],
    "brandNames": [],
    "subjectIds": [],
    "tagIds": [],
}


def post_with_retries(url, headers, json_body, retries=3, backoff=2, timeout=30):
    """POST request with retry logic for rate limiting / transient errors"""
    attempt = 0
    while True:
        try:
            resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
        except requests.RequestException as exc:
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
        return resp


def save_response_json(resp_json, filename=None):
    """Save response JSON to file in the same directory as this script"""
    if not filename:
        timestamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
        filename = f"cr_daily_stats_response_{timestamp}.json"
    out_path = os.path.join(SCRIPT_DIR, filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(resp_json, f, ensure_ascii=False, indent=2)
    return out_path


def _build_request_body(limit: int, offset: int) -> dict:
    # derive selected/past from payload.period
    sel_start_dt = datetime.strptime(payload["period"]["begin"], "%Y-%m-%d %H:%M:%S").astimezone(tz)
    sel_end_dt = datetime.strptime(payload["period"]["end"], "%Y-%m-%d %H:%M:%S").astimezone(tz)
    sel_start = to_api_date(sel_start_dt)
    sel_end = to_api_date(sel_end_dt)

    # same-length past period ending the day before selected start
    delta_days = (datetime.strptime(sel_end, "%Y-%m-%d") - datetime.strptime(sel_start, "%Y-%m-%d")).days + 1
    past_end_dt = datetime.strptime(sel_start, "%Y-%m-%d") - timedelta(days=1)
    past_start_dt = past_end_dt - timedelta(days=delta_days - 1)

    body = {
        "selectedPeriod": {"start": sel_start, "end": sel_end},
        "pastPeriod": {"start": past_start_dt.strftime("%Y-%m-%d"), "end": past_end_dt.strftime("%Y-%m-%d")},
        "nmIds": payload.get("nmIds", []),
        "brandNames": payload.get("brandNames", []),
        "subjectIds": payload.get("subjectIds", []),
        "tagIds": payload.get("tagIds", []),
        "skipDeletedNm": True,
        "orderBy": {"field": "openCard", "mode": "desc"},
        "limit": limit,
        "offset": offset,
    }
    return body


def _map_product_to_card(prod: dict) -> dict:
    nm_id = prod.get("product", {}).get("nmId")
    vendor_code = prod.get("product", {}).get("vendorCode")
    stocks = prod.get("stocks", {})
    stat_sel = prod.get("statistic", {}).get("selected", {})
    stat_past = prod.get("statistic", {}).get("past", {})

    def _map_period(stat: dict) -> dict:
        period = stat.get("period", {})
        conv = stat.get("conversions", {})
        return {
            "openCardCount": stat.get("openCount"),
            "addToCartCount": stat.get("cartCount"),
            "ordersCount": stat.get("orderCount"),
            "ordersSumRub": stat.get("orderSum"),
            "cancelCount": stat.get("cancelCount"),
            "conversions": {
                "addToCartPercent": conv.get("addToCartPercent"),
                "cartToOrderPercent": conv.get("cartToOrderPercent"),
            },
            # Мы не храним begin/end внутри блока периода, summary добавим сверху
        }

    card = {
        "nmID": int(nm_id) if nm_id is not None else None,
        "vendorCode": vendor_code or "",
        "statistics": {
            "selectedPeriod": _map_period(stat_sel),
            "previousPeriod": _map_period(stat_past),
        },
        "stocks": {
            "stocksMp": stocks.get("mp"),
            "stocksWb": stocks.get("wb"),
        },
    }
    return card


def fetch_cr_daily_stats():
    """Fetch CR stats via sales-funnel API and map to legacy structure."""
    print(f"🔄 Запрос статистики CR: {URL}")
    print(f"📅 Период: {payload['period']['begin']} → {payload['period']['end']}")

    limit = 100
    offset = 0
    all_products = []

    while True:
        body = _build_request_body(limit=limit, offset=offset)
        resp = post_with_retries(URL, HEADERS, body)
        if resp.status_code != 200:
            print(f"❌ Ошибка запроса: {resp.status_code}")
            try:
                print(resp.text)
            except Exception:
                pass
            resp.raise_for_status()

        js = resp.json()
        products = js.get("data", {}).get("products", [])
        all_products.extend(products)
        print(f"   ▸ Страница offset={offset}: получено {len(products)}")
        if len(products) < limit:
            break
        offset += limit
        time.sleep(2)

    # Map to legacy structure
    cards = [_map_product_to_card(p) for p in all_products]

    # Build summary with selected/past period boundaries
    sel_start_dt = datetime.strptime(payload["period"]["begin"], "%Y-%m-%d %H:%M:%S").astimezone(tz)
    sel_end_dt = datetime.strptime(payload["period"]["end"], "%Y-%m-%d %H:%M:%S").astimezone(tz)
    sel_start = to_api_date(sel_start_dt)
    sel_end = to_api_date(sel_end_dt)
    delta_days = (datetime.strptime(sel_end, "%Y-%m-%d") - datetime.strptime(sel_start, "%Y-%m-%d")).days + 1
    past_end_dt = datetime.strptime(sel_start, "%Y-%m-%d") - timedelta(days=1)
    past_start_dt = past_end_dt - timedelta(days=delta_days - 1)

    result = {
        "data": {
            "cards": cards,
            "summary": {
                "selectedPeriod": {
                    "begin": f"{sel_start} 00:00:00",
                    "end": f"{sel_end} 23:59:59",
                },
                "previousPeriod": {
                    "begin": f"{past_start_dt.strftime('%Y-%m-%d')} 00:00:00",
                    "end": f"{past_end_dt.strftime('%Y-%m-%d')} 23:59:59",
                },
            },
        }
    }

    print(f"✅ Получено карточек: {len(cards)}")
    return result


def main():
    """Main entry point for standalone execution"""
    data = fetch_cr_daily_stats()
    out_file = save_response_json(data)
    print(f"💾 JSON сохранен: {out_file}")
    return data


if __name__ == "__main__":
    main()

