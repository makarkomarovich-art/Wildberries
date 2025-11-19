"""
Агрегация данных для 4 уровней.
Преобразует сырые данные в форматы для каждого уровня.
"""

import logging
from datetime import date
from typing import Dict, List, Tuple, Any


def build_level_1_articulы(
    raw_data: Dict[Tuple[int, date], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Построение LEVEL 1 (vendor_code → "Артикул").
    БЕЗ GROUP BY, просто компоновка данных.
    
    Args:
        raw_data: {(nm_id, date): {attr: value}}
    
    Returns:
        Список словарей, готовых к записи в Google Sheets
    """
    logging.info("🔨 Построение LEVEL 1: Артикулы...")
    
    rows = []
    
    for (nm_id, row_date), attrs in raw_data.items():
        orders_sum = attrs.get('orders_sum_rub', 0)
        adv_spend = attrs.get('sum', 0)  # Расход на рекламу
        clicks = attrs.get('open_card_count', 0)  # Клики общие
        add_to_cart = attrs.get('add_to_cart_count', 0)  # В корзину
        orders = attrs.get('orders_count', 0)  # Заказы
        
        # ДРР = (Расход на рекламу / Сумма заказов) * 100
        drr = (adv_spend / orders_sum * 100) if orders_sum > 0 else 0
        
        row = {
            'date': row_date,
            'признак': 'Артикул',
            'имя': attrs.get('vendor_code', ''),
            
            # Метрики
            'orders_count': attrs.get('orders_count', 0),
            'orders_sum_rub': attrs.get('orders_sum_rub', 0),
            'sum': attrs.get('sum', 0),  # Расход на рекламу
            'drr': drr,  # ДРР (вычислено)
            'open_card_count': attrs.get('open_card_count', 0),  # Клики общие
            'add_to_cart_count': attrs.get('add_to_cart_count', 0),
            'add_to_cart_percent': attrs.get('add_to_cart_percent', 0),
            'cart_to_order_percent': attrs.get('cart_to_order_percent', 0),
            'cpm': attrs.get('cpm', 0),
            'views': attrs.get('views', 0),  # Рекламные просмотры
            'clicks': attrs.get('clicks', 0),  # Рекламные клики
            'ctr': attrs.get('ctr', 0),
            'cpc': attrs.get('cpc', 0),
            'order_price': attrs.get('order_price', 0),
        }
        rows.append(row)
    
    logging.info(f"✅ LEVEL 1: Построено {len(rows)} строк артикулов")
    return rows


def build_level_2_предметы(
    raw_data: Dict[Tuple[str, date], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Построение LEVEL 2 (category_wb → "Предмет").
    GROUP BY (category_wb, date) уже сделана в fetch.
    ТОЛЬКО: Заказы, Сумма заказов, Расход на рекламу, Клики общие, В корзину, Рекламные просмотры, Рекламные клики, CPM, CPC, ДРР, Конверсия в корзину, Конверсия в заказ, CTR
    
    Returns:
        Список словарей
    """
    logging.info("🔨 Построение LEVEL 2: Предметы (категории)...")
    
    rows = []
    
    for (category_wb, row_date), attrs in raw_data.items():
        sum_val = attrs.get('sum', 0)
        views_val = attrs.get('views', 0)
        clicks_val = attrs.get('clicks', 0)
        orders_sum = attrs.get('orders_sum_rub', 0)
        clicks_general = attrs.get('open_card_count', 0)  # Клики общие
        add_to_cart = attrs.get('add_to_cart_count', 0)   # В корзину
        orders = attrs.get('orders_count', 0)             # Заказы
        
        # Вычисляем CPM и CPC
        # CPM = (расходы / просмотры) * 1000
        cpm = (sum_val / views_val * 1000) if views_val > 0 else 0
        # CPC = расходы / клики
        cpc = (sum_val / clicks_val) if clicks_val > 0 else 0
        
        # ДРР = (Расход на рекламу / Сумма заказов) * 100
        drr = (sum_val / orders_sum * 100) if orders_sum > 0 else 0
        
        # Конверсия в корзину = (В корзину / Клики общие) * 100
        conv_to_cart = (add_to_cart / clicks_general * 100) if clicks_general > 0 else 0
        
        # Конверсия в заказ = (Заказы / В корзину) * 100
        conv_to_order = (orders / add_to_cart * 100) if add_to_cart > 0 else 0
        
        # CTR = (Рекламные клики / Рекламные просмотры) * 100
        ctr_val = (clicks_val / views_val * 100) if views_val > 0 else 0
        
        # Цена одного заказа = Сумма заказов / Заказы
        order_price = (orders_sum / orders) if orders > 0 else 0
        order_price = _round_percent(order_price, 2)
        
        row = {
            'date': row_date,
            'признак': 'Предмет',
            'имя': category_wb,
            
            # ТОЛЬКО нужные агрегированные метрики
            'orders_count': attrs.get('orders_count', 0),          # Заказы
            'orders_sum_rub': attrs.get('orders_sum_rub', 0),      # Сумма заказов
            'sum': attrs.get('sum', 0),                             # Расход на рекламу
            'drr': drr,                                             # ДРР (вычислено)
            'open_card_count': attrs.get('open_card_count', 0),    # Клики общие
            'add_to_cart_count': attrs.get('add_to_cart_count', 0),# В корзину
            'add_to_cart_percent': conv_to_cart,                    # Конверсия в корзину (вычислено)
            'cart_to_order_percent': conv_to_order,                 # Конверсия в заказ (вычислено)
            'cpm': cpm,                                             # CPM (вычислено)
            'views': attrs.get('views', 0),                         # Рекламные просмотры
            'clicks': attrs.get('clicks', 0),                       # Рекламные клики
            'ctr': ctr_val,                                         # CTR (вычислено)
            'cpc': cpc,                                             # CPC (вычислено)
            'order_price': order_price,                             # Цена одного заказа (вычислено)
        }
        rows.append(row)
    
    logging.info(f"✅ LEVEL 2: Построено {len(rows)} строк предметов")
    return rows


def build_level_3_склейки(
    raw_data: Dict[Tuple[int, date], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Построение LEVEL 3 (imt_id → "Склейка").
    GROUP BY (imt_id, date) уже сделана в fetch.
    ТОЛЬКО: Заказы, Сумма заказов, Расход на рекламу, Клики общие, В корзину, Рекламные просмотры, Рекламные клики, CPM, CPC, ДРР, Конверсия в корзину, Конверсия в заказ, CTR
    
    Returns:
        Список словарей
    """
    logging.info("🔨 Построение LEVEL 3: Склейки...")
    
    rows = []
    
    for (imt_id, row_date), attrs in raw_data.items():
        sum_val = attrs.get('sum', 0)
        views_val = attrs.get('views', 0)
        clicks_val = attrs.get('clicks', 0)
        orders_sum = attrs.get('orders_sum_rub', 0)
        clicks_general = attrs.get('open_card_count', 0)  # Клики общие
        add_to_cart = attrs.get('add_to_cart_count', 0)   # В корзину
        orders = attrs.get('orders_count', 0)             # Заказы
        
        # Вычисляем CPM и CPC
        # CPM = (расходы / просмотры) * 1000
        cpm = (sum_val / views_val * 1000) if views_val > 0 else 0
        # CPC = расходы / клики
        cpc = (sum_val / clicks_val) if clicks_val > 0 else 0
        
        # ДРР = (Расход на рекламу / Сумма заказов) * 100
        drr = (sum_val / orders_sum * 100) if orders_sum > 0 else 0
        
        # Конверсия в корзину = (В корзину / Клики общие) * 100
        conv_to_cart = (add_to_cart / clicks_general * 100) if clicks_general > 0 else 0
        
        # Конверсия в заказ = (Заказы / В корзину) * 100
        conv_to_order = (orders / add_to_cart * 100) if add_to_cart > 0 else 0
        
        # CTR = (Рекламные клики / Рекламные просмотры) * 100
        ctr_val = (clicks_val / views_val * 100) if views_val > 0 else 0
        
        # Цена одного заказа = Сумма заказов / Заказы
        order_price = (orders_sum / orders) if orders > 0 else 0
        order_price = _round_percent(order_price, 2)
        
        row = {
            'date': row_date,
            'признак': 'Склейка',
            'имя': str(imt_id),
            
            # ТОЛЬКО нужные агрегированные метрики
            'orders_count': attrs.get('orders_count', 0),          # Заказы
            'orders_sum_rub': attrs.get('orders_sum_rub', 0),      # Сумма заказов
            'sum': attrs.get('sum', 0),                             # Расход на рекламу
            'drr': drr,                                             # ДРР (вычислено)
            'open_card_count': attrs.get('open_card_count', 0),    # Клики общие
            'add_to_cart_count': attrs.get('add_to_cart_count', 0),# В корзину
            'add_to_cart_percent': conv_to_cart,                    # Конверсия в корзину (вычислено)
            'cart_to_order_percent': conv_to_order,                 # Конверсия в заказ (вычислено)
            'cpm': cpm,                                             # CPM (вычислено)
            'views': attrs.get('views', 0),                         # Рекламные просмотры
            'clicks': attrs.get('clicks', 0),                       # Рекламные клики
            'ctr': ctr_val,                                         # CTR (вычислено)
            'cpc': cpc,                                             # CPC (вычислено)
            'order_price': order_price,                             # Цена одного заказа (вычислено)
        }
        rows.append(row)
    
    logging.info(f"✅ LEVEL 3: Построено {len(rows)} строк склеек")
    return rows


def build_level_4_магазин(
    raw_data: Dict[Tuple[str, date], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Построение LEVEL 4 (магазин).
    GROUP BY (date) уже сделана в fetch. Ключ = ('Магазин', date).
    ТОЛЬКО: Заказы, Сумма заказов, Расход на рекламу, Клики общие, В корзину, Рекламные просмотры, Рекламные клики, CPM, CPC, ДРР, Конверсия в корзину, Конверсия в заказ, CTR
    
    Returns:
        Список словарей (будет 90 строк)
    """
    logging.info("🔨 Построение LEVEL 4: Магазин...")
    
    rows = []
    
    for (store_key, row_date), attrs in raw_data.items():
        sum_val = attrs.get('sum', 0)
        views_val = attrs.get('views', 0)
        clicks_val = attrs.get('clicks', 0)
        orders_sum = attrs.get('orders_sum_rub', 0)
        clicks_general = attrs.get('open_card_count', 0)  # Клики общие
        add_to_cart = attrs.get('add_to_cart_count', 0)   # В корзину
        orders = attrs.get('orders_count', 0)             # Заказы
        
        # Вычисляем CPM и CPC
        # CPM = (расходы / просмотры) * 1000
        cpm = (sum_val / views_val * 1000) if views_val > 0 else 0
        # CPC = расходы / клики
        cpc = (sum_val / clicks_val) if clicks_val > 0 else 0
        
        # ДРР = (Расход на рекламу / Сумма заказов) * 100
        drr = (sum_val / orders_sum * 100) if orders_sum > 0 else 0
        
        # Конверсия в корзину = (В корзину / Клики общие) * 100
        conv_to_cart = (add_to_cart / clicks_general * 100) if clicks_general > 0 else 0
        
        # Конверсия в заказ = (Заказы / В корзину) * 100
        conv_to_order = (orders / add_to_cart * 100) if add_to_cart > 0 else 0
        
        # CTR = (Рекламные клики / Рекламные просмотры) * 100
        ctr_val = (clicks_val / views_val * 100) if views_val > 0 else 0
        
        # Цена одного заказа = Сумма заказов / Заказы
        order_price = (orders_sum / orders) if orders > 0 else 0
        order_price = _round_percent(order_price, 2)
        
        row = {
            'date': row_date,
            'признак': 'Магазин',
            'имя': 'Магазин',
            
            # ТОЛЬКО нужные агрегированные метрики
            'orders_count': attrs.get('orders_count', 0),          # Заказы
            'orders_sum_rub': attrs.get('orders_sum_rub', 0),      # Сумма заказов
            'sum': attrs.get('sum', 0),                             # Расход на рекламу
            'drr': drr,                                             # ДРР (вычислено)
            'open_card_count': attrs.get('open_card_count', 0),    # Клики общие
            'add_to_cart_count': attrs.get('add_to_cart_count', 0),# В корзину
            'add_to_cart_percent': conv_to_cart,                    # Конверсия в корзину (вычислено)
            'cart_to_order_percent': conv_to_order,                 # Конверсия в заказ (вычислено)
            'cpm': cpm,                                             # CPM (вычислено)
            'views': attrs.get('views', 0),                         # Рекламные просмотры
            'clicks': attrs.get('clicks', 0),                       # Рекламные клики
            'ctr': ctr_val,                                         # CTR (вычислено)
            'cpc': cpc,                                             # CPC (вычислено)
            'order_price': order_price,                             # Цена одного заказа (вычислено)
        }
        rows.append(row)
    
    logging.info(f"✅ LEVEL 4: Построено {len(rows)} строк магазина")
    return rows


def combine_all_levels(
    level_1: List[Dict[str, Any]],
    level_2: List[Dict[str, Any]],
    level_3: List[Dict[str, Any]],
    level_4: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Объединение всех 4 уровней в один DataFrame.
    
    Returns:
        Объединённый список
    """
    logging.info("🔗 Объединение всех 4 уровней...")
    
    combined = level_1 + level_2 + level_3 + level_4
    
    # Сортируем по: признак, дата DESC (новые сверху), имя
    combined_sorted = sorted(
        combined,
        key=lambda x: (
            x['признак'],
            -x['date'].toordinal() if isinstance(x['date'], date) else 0,
            str(x.get('имя', ''))
        )
    )
    
    logging.info(f"✅ Объединено {len(combined_sorted)} строк из всех 4 уровней")
    return combined_sorted

