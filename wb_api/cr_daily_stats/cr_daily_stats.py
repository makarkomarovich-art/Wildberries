#!/usr/bin/env python3
"""
Запрос статистики CR (Conversion Rate) по артикулам за текущий день.
API возвращает selectedPeriod (сегодня) и previousPeriod (вчера).
"""
import os
import json
import time
from datetime import datetime
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
ENDPOINT_PATH = "/api/v2/nm-report/detail"
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
    """Convert datetime to API format: YYYY-MM-DD HH:MM:SS"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


payload = {
    "brandNames": [],
    "objectIDs": [],
    "tagIDs": [],
    "nmIDs": [],
    "timezone": TZ_NAME,
    "period": {
        "begin": to_api_datetime(begin_dt),
        "end": to_api_datetime(end_dt),
    },
    "orderBy": {"field": "openCard", "mode": "desc"},
    "page": 1,
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


def fetch_cr_daily_stats():
    """Fetch CR daily stats from WB API"""
    print(f"🔄 Запрос статистики CR: {URL}")
    print(f"📅 Период: {payload['period']['begin']} → {payload['period']['end']}")
    
    resp = post_with_retries(URL, HEADERS, payload)
    
    if resp.status_code != 200:
        print(f"❌ Ошибка запроса: {resp.status_code}")
        try:
            print(resp.text)
        except Exception:
            pass
        resp.raise_for_status()
    
    data = resp.json()
    print(f"✅ Получено карточек: {len(data.get('data', {}).get('cards', []))}")
    
    return data


def main():
    """Main entry point for standalone execution"""
    data = fetch_cr_daily_stats()
    out_file = save_response_json(data)
    print(f"💾 JSON сохранен: {out_file}")
    return data


if __name__ == "__main__":
    main()

