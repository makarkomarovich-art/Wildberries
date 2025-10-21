#!/usr/bin/env python3
"""
Трансформация данных из API /adv/v3/fullstats в структуру для БД.

Логика:
1. Парсим каждую кампанию -> день -> платформа -> артикул
2. Агрегируем метрики артикула по всем платформам (сумма views/clicks/sum)
3. Фильтруем: только артикулы с views > 0 (отсекаем склейку с нулевыми просмотрами)
4. Берем orders/sum_price из уровня дня (включает склейку)
5. Вычисляем производные метрики (CPM, CTR)
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List
from decimal import Decimal


class CampaignDailyStats:
    """Структура данных для одной записи в adv_campaign_daily_stats."""
    
    def __init__(
        self,
        advert_id: int,
        nm_id: int,
        vendor_code: str,
        date: date,
        views: int,
        clicks: int,
        sum: Decimal,
        cpc: Decimal | None,
        ctr: Decimal | None,
        orders: int,
        orders_sum: Decimal,
        cpm: Decimal | None
    ):
        self.advert_id = advert_id
        self.nm_id = nm_id
        self.vendor_code = vendor_code
        self.date = date
        self.views = views
        self.clicks = clicks
        self.sum = sum
        self.cpc = cpc
        self.ctr = ctr
        self.orders = orders
        self.orders_sum = orders_sum
        self.cpm = cpm
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертировать в dict для вставки в БД."""
        return {
            'advert_id': self.advert_id,
            'nm_id': self.nm_id,
            'vendor_code': self.vendor_code,
            'date': self.date.isoformat(),  # Конвертируем date в строку
            'views': self.views,
            'clicks': self.clicks,
            'sum': float(self.sum) if self.sum is not None else 0,
            'cpc': float(self.cpc) if self.cpc is not None else None,
            'ctr': float(self.ctr) if self.ctr is not None else None,
            'orders': self.orders,
            'orders_sum': float(self.orders_sum) if self.orders_sum is not None else 0,
            'cpm': float(self.cpm) if self.cpm is not None else None
        }
    
    def __repr__(self) -> str:
        return (f"CampaignDailyStats(advert_id={self.advert_id}, nm_id={self.nm_id}, "
                f"date={self.date}, views={self.views}, orders={self.orders})")


def transform_fullstats_to_campaign_daily(
    fullstats_response: List[Dict[str, Any]],
    vendor_code_map: Dict[int, str],
    *,
    min_views_threshold: int = 1
) -> List[CampaignDailyStats]:
    """
    Трансформировать ответ API fullstats в записи для adv_campaign_daily_stats.
    
    Args:
        fullstats_response: Ответ от API /adv/v3/fullstats
        vendor_code_map: Маппинг nm_id -> vendor_code (из таблицы products)
        min_views_threshold: Минимальное количество просмотров для включения артикула
    
    Returns:
        Список объектов CampaignDailyStats
    
    Raises:
        ValueError: При ошибках в данных
    """
    result = []
    
    for campaign in fullstats_response:
        advert_id = campaign.get('advertId')
        if not advert_id:
            continue
        
        days = campaign.get('days', [])
        
        for day in days:
            # Парсим дату
            date_str = day.get('date')
            if not date_str:
                continue
            
            day_date = _parse_date(date_str)
            
            # УРОВЕНЬ ДНЯ: Заказы со всей склейкой
            day_orders = day.get('orders', 0)
            day_sum_price = Decimal(str(day.get('sum_price', 0)))
            
            # Собираем метрики каждого артикула из всех платформ
            nm_metrics = _aggregate_nm_metrics_by_platforms(day.get('apps', []))
            
            # Фильтруем и создаем записи
            for nm_id, metrics in nm_metrics.items():
                if metrics['views'] < min_views_threshold:
                    continue
                
                # Проверяем наличие vendor_code
                vendor_code = vendor_code_map.get(nm_id)
                if not vendor_code:
                    # Артикул не найден в products - пропускаем или ошибка
                    print(f"⚠️  Warning: nm_id {nm_id} not found in products table, skipping")
                    continue
                
                # Вычисляем производные метрики
                cpc = _calculate_cpc(metrics['sum'], metrics['clicks'])
                ctr = _calculate_ctr(metrics['clicks'], metrics['views'])
                cpm = _calculate_cpm(metrics['sum'], metrics['views'])
                
                # Создаем запись
                stats = CampaignDailyStats(
                    advert_id=advert_id,
                    nm_id=nm_id,
                    vendor_code=vendor_code,
                    date=day_date,
                    views=metrics['views'],
                    clicks=metrics['clicks'],
                    sum=metrics['sum'],
                    cpc=cpc,
                    ctr=ctr,
                    orders=day_orders,
                    orders_sum=day_sum_price,
                    cpm=cpm
                )
                
                result.append(stats)
    
    return result


def _aggregate_nm_metrics_by_platforms(apps: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """
    Агрегировать метрики каждого артикула по всем платформам.
    
    Args:
        apps: Список платформ из day['apps']
    
    Returns:
        Dict: {nm_id: {'views': int, 'clicks': int, 'sum': Decimal, 'cpc_list': List[float]}}
    """
    nm_metrics: Dict[int, Dict[str, Any]] = {}
    
    for app in apps:
        nms = app.get('nms', [])
        
        for nm in nms:
            nm_id = nm.get('nmId')
            if not nm_id:
                continue
            
            # Инициализируем метрики если первая встреча
            if nm_id not in nm_metrics:
                nm_metrics[nm_id] = {
                    'views': 0,
                    'clicks': 0,
                    'sum': Decimal('0'),
                    'cpc_list': []
                }
            
            # Суммируем метрики
            nm_metrics[nm_id]['views'] += nm.get('views', 0)
            nm_metrics[nm_id]['clicks'] += nm.get('clicks', 0)
            nm_metrics[nm_id]['sum'] += Decimal(str(nm.get('sum', 0)))
            
            # Собираем CPC для средневзвешенного расчета
            cpc_value = nm.get('cpc', 0)
            if cpc_value and cpc_value > 0:
                nm_metrics[nm_id]['cpc_list'].append(float(cpc_value))
    
    return nm_metrics


def _parse_date(date_str: str) -> date:
    """
    Парсить дату из ISO формата.
    
    Examples:
        "2025-09-28T00:00:00Z" -> date(2025, 9, 28)
        "2025-09-28" -> date(2025, 9, 28)
    """
    try:
        # Пробуем ISO формат с временем
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return dt.date()
    except ValueError:
        # Пробуем простой формат даты
        return datetime.strptime(date_str, "%Y-%m-%d").date()


def _calculate_cpc(sum_value: Decimal, clicks: int) -> Decimal | None:
    """
    Рассчитать среднюю стоимость клика: sum / clicks.
    
    Returns:
        Decimal или None если clicks = 0
    """
    if clicks == 0:
        return None
    return (sum_value / Decimal(clicks)).quantize(Decimal('0.01'))


def _calculate_ctr(clicks: int, views: int) -> Decimal | None:
    """
    Рассчитать CTR: (clicks / views) * 100.
    
    Returns:
        Decimal (%) или None если views = 0
    """
    if views == 0:
        return None
    return ((Decimal(clicks) / Decimal(views)) * Decimal(100)).quantize(Decimal('0.01'))


def _calculate_cpm(sum_value: Decimal, views: int) -> Decimal | None:
    """
    Рассчитать CPM (Cost Per Mille): (sum / views) * 1000.
    
    Returns:
        Decimal или None если views = 0
    """
    if views == 0:
        return None
    return ((sum_value / Decimal(views)) * Decimal(1000)).quantize(Decimal('0.01'))


def get_transform_summary(stats_list: List[CampaignDailyStats]) -> Dict[str, Any]:
    """
    Получить сводку по трансформированным данным.
    
    Returns:
        Dict с метриками:
            - total_records: количество записей
            - unique_campaigns: уникальных кампаний
            - unique_articles: уникальных артикулов
            - date_range: диапазон дат
            - total_views: суммарные просмотры
            - total_orders: суммарные заказы
    """
    if not stats_list:
        return {
            'total_records': 0,
            'unique_campaigns': 0,
            'unique_articles': 0,
            'date_range': None,
            'total_views': 0,
            'total_orders': 0
        }
    
    unique_campaigns = set()
    unique_articles = set()
    dates = []
    total_views = 0
    total_orders = 0
    
    for stats in stats_list:
        unique_campaigns.add(stats.advert_id)
        unique_articles.add(stats.nm_id)
        dates.append(stats.date)
        total_views += stats.views
        total_orders += stats.orders
    
    return {
        'total_records': len(stats_list),
        'unique_campaigns': len(unique_campaigns),
        'unique_articles': len(unique_articles),
        'date_range': (min(dates), max(dates)) if dates else None,
        'total_views': total_views,
        'total_orders': total_orders
    }

