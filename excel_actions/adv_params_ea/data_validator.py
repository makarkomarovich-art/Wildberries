#!/usr/bin/env python3
"""
Валидация трансформированных данных перед загрузкой в БД.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List

from .transform import CampaignDailyStats


class DataValidationError(Exception):
    """Ошибка валидации данных."""
    pass


def validate_campaign_daily_stats(stats: CampaignDailyStats) -> None:
    """
    Валидировать одну запись CampaignDailyStats перед вставкой в БД.
    
    Args:
        stats: Объект с данными
    
    Raises:
        DataValidationError: При ошибке валидации
    """
    errors = []
    
    # Проверка обязательных полей
    if not stats.advert_id or stats.advert_id <= 0:
        errors.append(f"Invalid advert_id: {stats.advert_id}")
    
    if not stats.nm_id or stats.nm_id <= 0:
        errors.append(f"Invalid nm_id: {stats.nm_id}")
    
    if not stats.vendor_code or not stats.vendor_code.strip():
        errors.append(f"Empty vendor_code for nm_id {stats.nm_id}")
    
    if not isinstance(stats.date, date):
        errors.append(f"Invalid date type: {type(stats.date)}")
    
    # Проверка метрик (должны быть >= 0)
    if stats.views < 0:
        errors.append(f"Negative views: {stats.views}")
    
    if stats.clicks < 0:
        errors.append(f"Negative clicks: {stats.clicks}")
    
    if stats.sum < 0:
        errors.append(f"Negative sum: {stats.sum}")
    
    if stats.orders < 0:
        errors.append(f"Negative orders: {stats.orders}")
    
    if stats.orders_sum < 0:
        errors.append(f"Negative orders_sum: {stats.orders_sum}")
    
    # Проверка вычисляемых метрик (если есть)
    if stats.cpc is not None and stats.cpc < 0:
        errors.append(f"Negative cpc: {stats.cpc}")
    
    if stats.ctr is not None and (stats.ctr < 0 or stats.ctr > 100):
        errors.append(f"Invalid ctr (must be 0-100%): {stats.ctr}")
    
    if stats.cpm is not None and stats.cpm < 0:
        errors.append(f"Negative cpm: {stats.cpm}")
    
    # Логические проверки
    if stats.clicks > stats.views:
        errors.append(f"Clicks ({stats.clicks}) > views ({stats.views})")
    
    if stats.cpc is not None and stats.clicks == 0:
        errors.append(f"CPC is set but clicks = 0")
    
    if errors:
        raise DataValidationError(f"Validation failed for {stats}: " + "; ".join(errors))


def validate_campaign_daily_stats_batch(
    stats_list: List[CampaignDailyStats],
    *,
    raise_on_error: bool = True
) -> Dict[str, Any]:
    """
    Валидировать список записей.
    
    Args:
        stats_list: Список объектов CampaignDailyStats
        raise_on_error: Выбросить исключение при первой ошибке
    
    Returns:
        Dict с результатами:
            - valid: bool (все ли записи валидны)
            - total: int (общее количество)
            - valid_count: int (количество валидных)
            - invalid_count: int (количество невалидных)
            - errors: List[str] (список ошибок)
    
    Raises:
        DataValidationError: Если raise_on_error=True и есть ошибки
    """
    errors = []
    valid_count = 0
    invalid_count = 0
    
    for i, stats in enumerate(stats_list):
        try:
            validate_campaign_daily_stats(stats)
            valid_count += 1
        except DataValidationError as e:
            invalid_count += 1
            error_msg = f"Record {i+1}/{len(stats_list)}: {e}"
            errors.append(error_msg)
            
            if raise_on_error:
                raise DataValidationError(error_msg) from e
    
    return {
        'valid': invalid_count == 0,
        'total': len(stats_list),
        'valid_count': valid_count,
        'invalid_count': invalid_count,
        'errors': errors
    }


def check_for_duplicates(stats_list: List[CampaignDailyStats]) -> List[str]:
    """
    Проверить наличие дубликатов (advert_id, nm_id, date).
    
    Args:
        stats_list: Список объектов CampaignDailyStats
    
    Returns:
        Список сообщений о дубликатах
    """
    seen = set()
    duplicates = []
    
    for stats in stats_list:
        key = (stats.advert_id, stats.nm_id, stats.date)
        
        if key in seen:
            duplicates.append(
                f"Duplicate: advert_id={stats.advert_id}, nm_id={stats.nm_id}, date={stats.date}"
            )
        else:
            seen.add(key)
    
    return duplicates

