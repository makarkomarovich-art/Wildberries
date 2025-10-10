#!/usr/bin/env python3
import sys
import os
import json
from datetime import datetime, timedelta

# Ensure project root is on sys.path so api_keys can be imported
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    import api_keys
except Exception as e:
    print('Failed to import api_keys:', e)
    raise

import requests

API_URL = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/grouped/history'

def get_wb_token():
    token = getattr(api_keys, 'WB_API_TOKEN', None)
    if not token:
        raise RuntimeError('WB_API_TOKEN not found in api_keys')
    return token


def build_default_period():
    # last 7 days (maximum allowed period)
    end = datetime.now()
    begin = end - timedelta(days=7)
    return begin.date().isoformat(), end.date().isoformat()


def fetch_grouped_history(begin=None, end=None, objectIDs=None, brandNames=None, tagIDs=None, timezone='Europe/Moscow', aggregationLevel='day'):
    if begin is None or end is None:
        begin, end = build_default_period()

    payload = {
        'objectIDs': objectIDs or [],
        'brandNames': brandNames or [],
        'tagIDs': tagIDs or [],
        'period': {
            'begin': begin,
            'end': end,
            'timezone': timezone,
        },
        'aggregationLevel': aggregationLevel,
    }

    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {get_wb_token()}'
    }

    resp = requests.post(API_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


if __name__ == '__main__':
    out_path = os.path.join(os.path.dirname(__file__), 'CR_small_all_response.json')
    try:
        data = fetch_grouped_history()
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print('Saved response to', out_path)
    except Exception as e:
        print('Error fetching grouped history:', e)
        raise



