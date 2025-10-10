#!/usr/bin/env python3
import os
import json
import time
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import requests
import sys

# Ensure project root is on sys.path so `import api_keys` works when running from test_new_methods
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import API keys and settings from project file
import api_keys

# Config — avoid hardcoding endpoint pieces
BASE_URL = os.getenv("WB_BASE_URL", "https://seller-analytics-api.wildberries.ru")
ENDPOINT_PATH = "/api/v2/nm-report/detail"
URL = BASE_URL.rstrip("/") + ENDPOINT_PATH

# Choose token from api_keys (use only WB_API_TOKEN per user request)
TOKEN = getattr(api_keys, "WB_API_TOKEN", None)
USER_AGENT = getattr(api_keys, "USER_AGENT", "CR_big/1.0")
COOKIES = getattr(api_keys, "COOKIES", None)

if not TOKEN:
    raise RuntimeError("API token not found in api_keys.py (AUTHORIZEV3_TOKEN or WB_API_TOKEN)")

HEADERS = {
    "Authorization": TOKEN,
    "Content-Type": "application/json",
    "User-Agent": USER_AGENT,
}

if COOKIES:
    # requests accepts cookies as header too
    HEADERS["Cookie"] = COOKIES

# Default: last 7 days (end = now in Europe/Moscow)
TZ_NAME = os.getenv("WB_TIMEZONE", "Europe/Moscow")
if ZoneInfo:
    tz = ZoneInfo(TZ_NAME)
else:
    tz = None

now = datetime.now(tz) if tz is not None else datetime.utcnow()
begin_dt = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
end_dt = now.replace(microsecond=0)

# ISO format with offset when tz is available
def to_api_datetime(dt: datetime) -> str:
    # API expects format: "2006-01-02 15:04:05" (YYYY-MM-DD HH:MM:SS)
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
    # default sorting: openCard desc
    "orderBy": {"field": "openCard", "mode": "desc"},
    "page": 1,
}

# Simple retry logic in case of rate limiting / transient errors
def post_with_retries(url, headers, json_body, retries=3, backoff=2, timeout=30):
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

        # On success or client error, return response
        if resp.status_code == 200:
            return resp
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
            # respect Retry-After if provided
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
        # otherwise return (will raise below if not OK)
        return resp

def save_response_json(resp_json, filename=None):
    if not filename:
        b = begin_dt.date().isoformat()
        e = end_dt.date().isoformat()
        filename = f"CR_big_{b}_to_{e}.json"
    out_path = os.path.abspath(filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(resp_json, f, ensure_ascii=False, indent=2)
    return out_path

def main():
    print("Requesting report:", URL)
    print("Period:", payload["period"]["begin"], "->", payload["period"]["end"]) 
    resp = post_with_retries(URL, HEADERS, payload)
    if resp.status_code != 200:
        print("Request failed:", resp.status_code)
        try:
            print(resp.text)
        except Exception:
            pass
        resp.raise_for_status()
    data = resp.json()
    out_file = save_response_json(data)
    print("Saved JSON to:", out_file)

if __name__ == "__main__":
    main()


