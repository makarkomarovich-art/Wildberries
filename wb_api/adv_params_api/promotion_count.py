#!/usr/bin/env python3
"""
API client for /adv/v1/promotion/count - получение списка рекламных кампаний.

Возвращает список всех кампаний продавца с ID и временем последнего изменения.
Rate limit: 5 запросов/сек.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

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
ENDPOINT_PATH = "/adv/v1/promotion/count"
URL = f"{WB_ADV_API_BASE}{ENDPOINT_PATH}"


def fetch_promotion_count(
    token: str = WB_API_TOKEN,
    *,
    timeout_seconds: int = 30,
    save_response: bool = False,
    output_dir: Path | None = None
) -> Dict[str, Any]:
    """
    Получить список всех рекламных кампаний.
    
    Args:
        token: API токен
        timeout_seconds: Таймаут запроса
        save_response: Сохранить ответ в JSON файл
        output_dir: Директория для сохранения (по умолчанию - рядом со скриптом)
    
    Returns:
        Dict с полями:
            - all: общее количество кампаний
            - adverts: список групп кампаний по типу и статусу
    
    Raises:
        requests.RequestException: При ошибке HTTP запроса
    """
    headers = {"Authorization": token}
    
    try:
        response = requests.get(URL, headers=headers, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
        
        # Save response if requested
        if save_response:
            if output_dir is None:
                output_dir = SCRIPT_DIR
            _save_json_response(data, output_dir)
        
        return data
        
    except requests.RequestException as e:
        raise RuntimeError(f"Error fetching promotion count from {URL}: {e}") from e


def extract_campaign_ids(response: Dict[str, Any], filter_statuses: list[int] = None) -> list[int]:
    """
    Извлечь все advertId из ответа API.
    
    Args:
        response: Ответ от fetch_promotion_count()
        filter_statuses: Список статусов для фильтрации (например, [7, 9, 11])
                        Если None - берутся все кампании
    
    Returns:
        Список ID кампаний
    """
    campaign_ids = []
    
    for advert_group in response.get("adverts", []):
        status = advert_group.get("status")
        
        # Фильтрация по статусам
        if filter_statuses is not None and status not in filter_statuses:
            continue
        
        for campaign in advert_group.get("advert_list", []):
            if "advertId" in campaign:
                campaign_ids.append(campaign["advertId"])
    
    return campaign_ids


def get_campaigns_stats(response: Dict[str, Any]) -> Dict[str, Dict[str, int]]:
    """
    Получить статистику по кампаниям (по статусам и типам).
    
    Args:
        response: Ответ от fetch_promotion_count()
    
    Returns:
        Dict с ключами:
            - by_status: {7: count, 9: count, 11: count}
            - by_type: {8: count, 9: count, ...}
            - total: общее количество
    """
    by_status = {}
    by_type = {}
    
    for advert_group in response.get("adverts", []):
        status = advert_group.get("status")
        adv_type = advert_group.get("type")
        count = advert_group.get("count", 0)
        
        by_status[status] = by_status.get(status, 0) + count
        by_type[adv_type] = by_type.get(adv_type, 0) + count
    
    return {
        "by_status": by_status,
        "by_type": by_type,
        "total": response.get("all", 0)
    }


def _save_json_response(data: Any, directory: Path) -> Path:
    """Сохранить JSON ответ в файл."""
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"promotion_count_response_{timestamp}.json"
    filepath = directory / filename
    
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return filepath


# Для standalone выполнения
if __name__ == "__main__":
    print(f"🔄 Fetching campaigns list from {URL}")
    
    data = fetch_promotion_count(save_response=True)
    stats = get_campaigns_stats(data)
    campaign_ids = extract_campaign_ids(data)
    
    print(f"✅ Total campaigns: {stats['total']}")
    print(f"📊 By status: {stats['by_status']}")
    print(f"📊 By type: {stats['by_type']}")
    print(f"🆔 Campaign IDs extracted: {len(campaign_ids)}")
    print(f"   First 5: {campaign_ids[:5]}")

