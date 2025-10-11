#!/usr/bin/env python3
"""
Функции для записи рекламных данных в Supabase.

Отвечает за:
- Загрузку vendor_code из таблицы products
- Upsert записей в таблицу adv_campaign_daily_stats
- Запуск агрегации для adv_params
"""

from __future__ import annotations

from typing import Dict, List
from supabase import Client

from .transform import CampaignDailyStats


def get_vendor_code_map(supabase: Client) -> Dict[int, str]:
    """
    Получить маппинг nm_id -> vendor_code из таблицы products.
    
    Args:
        supabase: Клиент Supabase
    
    Returns:
        Dict: {nm_id: vendor_code}
    """
    print("🔄 Получение vendor_code из таблицы products...")
    
    response = supabase.table('products').select('nm_id, vendor_code').execute()
    vendor_map = {p['nm_id']: p['vendor_code'] for p in response.data}
    
    print(f"✅ Загружено products: {len(vendor_map)}")
    
    return vendor_map


def upsert_campaign_daily_stats(
    stats_list: List[CampaignDailyStats],
    supabase: Client
) -> int:
    """
    Upsert записей в таблицу adv_campaign_daily_stats.
    
    Args:
        stats_list: Список объектов CampaignDailyStats
        supabase: Клиент Supabase
    
    Returns:
        Количество обработанных записей
    
    Raises:
        Exception: При ошибке записи в БД
    """
    if not stats_list:
        print("⚠️  Нет записей для upsert")
        return 0
    
    print(f"🔄 Upsert в adv_campaign_daily_stats...")
    print(f"   Записей: {len(stats_list)}")
    
    # Конвертируем в dict для Supabase
    records = [stats.to_dict() for stats in stats_list]
    
    try:
        response = supabase.table('adv_campaign_daily_stats').upsert(
            records,
            on_conflict='advert_id,nm_id,date'
        ).execute()
        
        count = len(response.data) if response.data else len(records)
        print(f"✅ Обработано записей в adv_campaign_daily_stats: {count}")
        
        return count
    
    except Exception as e:
        print(f"❌ ОШИБКА при upsert в adv_campaign_daily_stats: {e}")
        raise


def trigger_adv_params_aggregation(
    supabase: Client,
    *,
    date_from: str | None = None,
    date_to: str | None = None
) -> int:
    """
    Запустить агрегацию данных в таблицу adv_params.
    
    Удаляет старые записи за период и создает новые агрегированные данные
    из adv_campaign_daily_stats.
    
    Args:
        supabase: Клиент Supabase
        date_from: Начало периода (YYYY-MM-DD) или None для всех дат
        date_to: Конец периода (YYYY-MM-DD) или None для всех дат
    
    Returns:
        Количество обработанных записей
    """
    print("🔄 Запуск агрегации для adv_params...")
    
    # Вызываем RPC функцию для агрегации
    # Функция сама делает UPSERT с сохранением created_at
    try:
        if date_from and date_to:
            print(f"   Период: {date_from} → {date_to}")
        response = supabase.rpc(
            'aggregate_adv_params',
            {
                'p_date_from': date_from,
                'p_date_to': date_to
            }
        ).execute()
        
        # Функция теперь возвращает INTEGER (количество записей)
        count = response.data if isinstance(response.data, int) else (response.data[0] if response.data else 0)
        print(f"✅ Агрегировано записей в adv_params: {count}")
        
        return count
    
    except Exception as e:
        # Тихо выбрасываем исключение для fallback на Python агрегацию
        raise


def insert_adv_params_direct(
    supabase: Client,
    date_from: str | None = None,
    date_to: str | None = None
) -> int:
    """
    Прямая вставка агрегированных данных в adv_params (без RPC).
    Используется если RPC функция недоступна.
    
    Args:
        supabase: Клиент Supabase
        date_from: Начало периода
        date_to: Конец периода
    
    Returns:
        Количество записей
    """
    print("🔄 Прямая агрегация для adv_params...")
    
    # Получаем детальные данные
    query = supabase.table('adv_campaign_daily_stats').select('*')
    
    if date_from:
        query = query.gte('date', date_from)
    if date_to:
        query = query.lte('date', date_to)
    
    response = query.execute()
    detailed_stats = response.data
    
    if not detailed_stats:
        print("⚠️  Нет данных для агрегации")
        return 0
    
    # Агрегируем в памяти
    from collections import defaultdict
    from decimal import Decimal
    
    aggregated = defaultdict(lambda: {
        'views': 0,
        'clicks': 0,
        'sum': Decimal('0'),
        'orders': 0,
        'orders_sum': Decimal('0')
    })
    
    for row in detailed_stats:
        key = (row['nm_id'], row['vendor_code'], row['date'])
        agg = aggregated[key]
        
        agg['views'] += row['views']
        agg['clicks'] += row['clicks']
        agg['sum'] += Decimal(str(row['sum']))
        agg['orders'] += row['orders']
        agg['orders_sum'] += Decimal(str(row['orders_sum']))
    
    # Конвертируем в записи для БД
    records = []
    for (nm_id, vendor_code, date), agg in aggregated.items():
        # Вычисляем метрики
        cpc = (agg['sum'] / agg['clicks']).quantize(Decimal('0.01')) if agg['clicks'] > 0 else None
        cpm = ((agg['sum'] / agg['views']) * 1000).quantize(Decimal('0.01')) if agg['views'] > 0 else None
        ctr = ((Decimal(agg['clicks']) / agg['views']) * 100).quantize(Decimal('0.01')) if agg['views'] > 0 else None
        
        records.append({
            'nm_id': nm_id,
            'vendor_code': vendor_code,
            'date': date,
            'views': agg['views'],
            'clicks': agg['clicks'],
            'sum': float(agg['sum']),
            'cpc': float(cpc) if cpc else None,
            'cpm': float(cpm) if cpm else None,
            'ctr': float(ctr) if ctr else None,
            'orders': agg['orders'],
            'orders_sum': float(agg['orders_sum'])
        })
    
    # Этот код используется только если RPC функция недоступна
    # Обычно не должен вызываться
    print("⚠️  Fallback: используется Python агрегация вместо RPC")
    
    # Простой UPSERT без сохранения created_at
    # (RPC функция работает лучше)
    if records:
        response = supabase.table('adv_params').upsert(
            records,
            on_conflict='nm_id,date'
        ).execute()
        
        count = len(response.data) if response.data else len(records)
        print(f"✅ Вставлено записей в adv_params (fallback): {count}")
        return count
    
    return 0

