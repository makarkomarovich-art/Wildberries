#!/usr/bin/env python3
"""
API client for /adv/v3/fullstats - получение детальной статистики по рекламным кампаниям.

Возвращает подробную статистику с разбивкой по дням, платформам и артикулам.
Rate limit: 3 запроса/мин (1 запрос/20 сек).
Max period: 31 день.
Max campaigns per request: 100.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, List

import requests

# Ensure project root is on sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from api_keys import WB_API_TOKEN
except ImportError as exc:
    raise RuntimeError("Failed to import WB_API_TOKEN from api_keys.py") from exc


# Configuration
WB_ADV_API_BASE = os.getenv("WB_ADV_API_BASE", "https://advert-api.wildberries.ru")
ENDPOINT_PATH = "/adv/v3/fullstats"
URL = f"{WB_ADV_API_BASE}{ENDPOINT_PATH}"


def fetch_fullstats(
    campaign_ids: List[int],
    begin_date: date,
    end_date: date,
    token: str = WB_API_TOKEN,
    *,
    timeout_seconds: int = 60,
    retry: bool = True,
    max_retries: int = 2,
    backoff_seconds: int = 65,
    save_response: bool = False,
    output_dir: Path | None = None
) -> List[dict]:
    """
    Получить детальную статистику по кампаниям.
    
    Args:
        campaign_ids: Список ID кампаний (максимум 100)
        begin_date: Начало периода
        end_date: Конец периода (максимум 31 день от начала)
        token: API токен
        timeout_seconds: Таймаут запроса
        retry: Повторять при ошибках 429/503
        max_retries: Максимум попыток
        backoff_seconds: Задержка между попытками
        save_response: Сохранить ответ в JSON файл
        output_dir: Директория для сохранения
    
    Returns:
        List кампаний с детальной статистикой
    
    Raises:
        ValueError: При невалидных параметрах
        RuntimeError: При ошибке HTTP запроса
    """
    # Validation
    if not campaign_ids:
        raise ValueError("campaign_ids cannot be empty")
    if len(campaign_ids) > 100:
        raise ValueError(f"Too many campaign IDs: {len(campaign_ids)}. Max 100 per request")
    if begin_date > end_date:
        raise ValueError("begin_date must be <= end_date")
    
    delta_days = (end_date - begin_date).days + 1
    if delta_days > 31:
        raise ValueError(f"Period too long: {delta_days} days. Max 31 days")
    
    # Prepare request
    headers = {"Authorization": token}
    params = {
        "ids": ",".join(str(cid) for cid in campaign_ids),
        "beginDate": begin_date.isoformat(),
        "endDate": end_date.isoformat()
    }
    
    # Execute request with retry logic
    attempts = 0
    while True:
        attempts += 1
        
        try:
            response = requests.get(URL, headers=headers, params=params, timeout=timeout_seconds)
            
            if response.status_code == 200:
                data = response.json()
                
                # Save response if requested
                if save_response:
                    if output_dir is None:
                        output_dir = SCRIPT_DIR
                    _save_json_response(data, output_dir, begin_date, end_date)
                
                return data
            
            # Handle rate limiting and transient errors
            if retry and attempts <= max_retries:
                if response.status_code in (429, 503):
                    print(f"⚠️  Rate limit/Service unavailable (attempt {attempts}/{max_retries+1}). "
                          f"Waiting {backoff_seconds}s...")
                    time.sleep(backoff_seconds)
                    continue
            
            # Non-retryable error or max retries exceeded
            error_details = _format_error_details(response)
            raise RuntimeError(
                f"Request failed (status={response.status_code}, attempts={attempts}). {error_details}"
            )
            
        except requests.RequestException as e:
            if retry and attempts <= max_retries:
                print(f"⚠️  Request exception (attempt {attempts}/{max_retries+1}): {e}")
                time.sleep(backoff_seconds)
                continue
            raise RuntimeError(f"Request failed after {attempts} attempts: {e}") from e


def fetch_fullstats_batch(
    campaign_ids: List[int],
    begin_date: date,
    end_date: date,
    token: str = WB_API_TOKEN,
    *,
    batch_size: int = 100,
    delay_between_batches: int = 65,
    **kwargs
) -> List[dict]:
    """
    Получить статистику для большого количества кампаний (автоматическая разбивка на батчи).
    
    Args:
        campaign_ids: Список ID кампаний (любое количество)
        begin_date: Начало периода
        end_date: Конец периода
        token: API токен
        batch_size: Размер батча (макс 100)
        delay_between_batches: Задержка между батчами (сек)
        **kwargs: Дополнительные параметры для fetch_fullstats()
    
    Returns:
        Объединенный список всех кампаний
    """
    if batch_size > 100:
        raise ValueError(f"batch_size cannot exceed 100, got {batch_size}")
    
    all_data = []
    total_batches = (len(campaign_ids) + batch_size - 1) // batch_size
    
    for i in range(0, len(campaign_ids), batch_size):
        batch_ids = campaign_ids[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        print(f"📦 Fetching batch {batch_num}/{total_batches} ({len(batch_ids)} campaigns)...")
        
        batch_data = fetch_fullstats(
            batch_ids,
            begin_date,
            end_date,
            token,
            **kwargs
        )
        all_data.extend(batch_data)
        
        # Delay between batches (except last one)
        if i + batch_size < len(campaign_ids):
            print(f"⏳ Waiting {delay_between_batches}s before next batch...")
            time.sleep(delay_between_batches)
    
    return all_data


def _format_error_details(response: requests.Response) -> str:
    """Форматировать детали ошибки из ответа."""
    try:
        payload = response.json()
        return f"Response: {json.dumps(payload, ensure_ascii=False)[:500]}"
    except Exception:
        return f"Response text: {response.text[:500]}"


def _save_json_response(data: Any, directory: Path, begin_date: date, end_date: date) -> Path:
    """Сохранить JSON ответ в файл."""
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"fullstats_{begin_date}_{end_date}_{timestamp}.json"
    filepath = directory / filename
    
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"💾 Saved response to {filepath}")
    return filepath


# Для standalone выполнения
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch WB advertising fullstats")
    parser.add_argument("--ids", nargs="+", type=int, help="Campaign IDs")
    parser.add_argument("--begin", required=True, help="Begin date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--save", action="store_true", help="Save response to file")
    
    args = parser.parse_args()
    
    begin = datetime.strptime(args.begin, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()
    
    print(f"🔄 Fetching fullstats from {URL}")
    print(f"📅 Period: {begin} → {end}")
    print(f"🆔 Campaigns: {args.ids}")
    
    data = fetch_fullstats(
        args.ids,
        begin,
        end,
        save_response=args.save
    )
    
    print(f"✅ Received {len(data)} campaigns")
    for campaign in data[:3]:
        print(f"   Campaign {campaign['advertId']}: {len(campaign.get('days', []))} days")

