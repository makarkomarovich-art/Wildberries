"""
Трансформация данных CR Daily Stats из API WB в формат для Supabase.

ЛОГИКА ВРЕМЕННЫХ МЕТОК И ДАТ:
================================

1. date_of_period (DATE) - бизнес-дата:
   - К какому дню относятся метрики (2025-10-10, 2025-10-09)
   - Определяется так:
     * selectedPeriod → используем begin из API (дата Europe/Moscow)
     * previousPeriod → строго на 1 день раньше, чем selectedPeriod.begin
   - Если begin недоступен, делаем fallback: now() и now()-1 день

2. created_at, updated_at (TIMESTAMPTZ) - технические метки:
   - Управляются автоматически PostgreSQL (DEFAULT NOW())
   - created_at: фиксируется один раз при INSERT
   - updated_at: обновляется при каждом UPDATE

3. Upsert ключ: (nm_id, date_of_period)
   - При повторном запуске в тот же день:
     * Запись за сегодня → обновляется (свежие метрики)
     * Запись за вчера → обновляется (если API вернёт другие данные)


ЛОГИКА STOCKS (ОСТАТКИ НА СКЛАДАХ):
====================================

ВАЖНО: stocks записываем ТОЛЬКО для сегодняшней записи!

- selectedPeriod (today) → включаем stocks_mp, stocks_wb
- previousPeriod (yesterday) → НЕ включаем stocks вообще

Причина: stocks - это актуальные остатки на момент запроса API.
Если записать сегодняшние stocks для вчерашней даты, данные сдвинутся на день.
При upsert вчерашней записи без полей stocks → PostgreSQL оставит значение,
которое было записано вчера, когда тот день был "сегодня".
"""

from __future__ import annotations
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo


# Timezone для вычисления дат
TZ = ZoneInfo("Europe/Moscow")


def calculate_order_price(period_data: dict) -> float | None:
    """
    Вычисляет среднюю цену заказа: orders_sum_rub / orders_count
    
    Args:
        period_data: Данные selectedPeriod или previousPeriod
    
    Returns:
        Средняя цена заказа или None если orders_count = 0/NULL
    """
    count = period_data.get('ordersCount')
    sum_rub = period_data.get('ordersSumRub')
    
    if count and count > 0 and sum_rub is not None:
        return round(sum_rub / count, 2)
    return None


def build_record(
    nm_id: int,
    vendor_code: str,
    date: str,
    period_data: dict,
    stocks: dict | None = None
) -> dict:
    """
    Создает одну запись для БД из данных периода.
    
    Args:
        nm_id: Артикул WB
        vendor_code: Артикул продавца
        date: Дата в формате 'YYYY-MM-DD' (date_of_period)
        period_data: Данные selectedPeriod или previousPeriod
        stocks: Данные stocks (включать только для сегодняшней записи!)
    
    Returns:
        Словарь с данными для upsert в Supabase
    """
    conversions = period_data.get('conversions', {})
    
    record = {
        'nm_id': nm_id,
        'vendor_code': vendor_code,
        'date_of_period': date,
        
        # Метрики: счетчики
        'open_card_count': period_data.get('openCardCount'),
        'add_to_cart_count': period_data.get('addToCartCount'),
        'orders_count': period_data.get('ordersCount'),
        'cancel_count': period_data.get('cancelCount'),
        
        # Метрики: суммы
        'orders_sum_rub': period_data.get('ordersSumRub'),
        
        # Конверсии
        'add_to_cart_percent': conversions.get('addToCartPercent'),
        'cart_to_order_percent': conversions.get('cartToOrderPercent'),
        
        # Агрегаты (вычисляем)
        'order_price': calculate_order_price(period_data),
    }
    
    # ВАЖНО: stocks добавляем ТОЛЬКО если переданы (только для today)
    if stocks is not None:
        record['stocks_mp'] = stocks.get('stocksMp')
        record['stocks_wb'] = stocks.get('stocksWb')
    
    return record


def extract_cr_stats_for_supabase(
    api_response: dict
) -> Tuple[List[dict], List[dict]]:
    """
    Извлекает и трансформирует данные из ответа API для записи в Supabase.
    
    Для каждой карточки создаем ДВЕ записи:
    1. За сегодня (selectedPeriod) - с полями stocks
    2. За вчера (previousPeriod) - БЕЗ полей stocks
    
    Args:
        api_response: Полный ответ от API nm-report/detail
    
    Returns:
        Кортеж (records_today, records_yesterday)
    """
    # Базовая дата из API: data.summary.selectedPeriod.begin (YYYY-MM-DD HH:MM:SS)
    base_selected: date | None = None
    try:
        sel_begin = api_response.get('data', {}).get('summary', {}).get('selectedPeriod', {}).get('begin')
        if isinstance(sel_begin, str) and len(sel_begin) >= 10:
            base_selected = date.fromisoformat(sel_begin[:10])
    except Exception:
        base_selected = None

    if base_selected is None:
        base_selected = datetime.now(TZ).date()

    today_str = str(base_selected)
    yesterday_str = str(base_selected - timedelta(days=1))
    
    records_today = []
    records_yesterday = []
    
    cards = api_response.get('data', {}).get('cards', [])
    
    for card in cards:
        nm_id = card.get('nmID')
        vendor_code = card.get('vendorCode')
        
        if not nm_id or not vendor_code:
            continue
        
        statistics = card.get('statistics', {})
        stocks = card.get('stocks', {})
        
        selected_period = statistics.get('selectedPeriod', {})
        # Фильтр "нулевых" карточек: если в selected всё нули/None — пропускаем карточку целиком
        if selected_period:
            oc = selected_period.get('openCardCount') or 0
            ac = selected_period.get('addToCartCount') or 0
            od = selected_period.get('ordersCount') or 0
            os = selected_period.get('ordersSumRub') or 0
            cc = selected_period.get('cancelCount') or 0
            if oc == 0 and ac == 0 and od == 0 and os == 0 and cc == 0:
                continue

        # Запись за СЕГОДНЯ (с stocks)
        if selected_period:
            record_today = build_record(
                nm_id=nm_id,
                vendor_code=vendor_code,
                date=today_str,
                period_data=selected_period,
                stocks=stocks  # ✅ Включаем stocks
            )
            records_today.append(record_today)
        
        # Запись за ВЧЕРА (БЕЗ stocks)
        previous_period = statistics.get('previousPeriod', {})
        if previous_period:
            record_yesterday = build_record(
                nm_id=nm_id,
                vendor_code=vendor_code,
                date=yesterday_str,
                period_data=previous_period,
                stocks=None  # ❌ НЕ включаем stocks
            )
            records_yesterday.append(record_yesterday)
    
    return records_today, records_yesterday

