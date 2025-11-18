"""
Fetch данных из Supabase и Google Sheets.
Получает сырые данные для каждого из 4 уровней агрегации.
"""

import logging
import psycopg2
from datetime import date, timedelta
from typing import Dict, List, Tuple, Any, Set
import decimal


def get_db_connection(db_url: str):
    """Инициализирует и возвращает подключение к базе данных."""
    conn = psycopg2.connect(db_url)
    return conn


def fetch_date_range(days_back: int) -> Tuple[date, date]:
    """
    Вычисляет диапазон дат для выборки.
    
    Args:
        days_back: Количество дней назад от сегодня
    
    Returns:
        (start_date, end_date)
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back - 1)  # -1 чтобы включить оба дня
    
    logging.info(f"📅 Диапазон дат: {start_date} → {end_date} ({days_back} дней)")
    return start_date, end_date


def fetch_raw_data_for_level_1(
    date_range: Tuple[date, date],
    db_url: str
) -> Dict[Tuple[int, date], Dict[str, Any]]:
    """
    Fetch сырых данных для LEVEL 1 (vendor_code, "Артикул").
    БЕЗ GROUP BY, просто JOIN cr_daily_stats + adv_params.
    
    Returns:
        {(nm_id, date): {attr: value}}
    """
    logging.info("🔍 LEVEL 1: Fetch данных по артикулам (vendor_code)...")
    
    start_date, end_date = date_range
    
    query = f"""
    SELECT
        COALESCE(cr.nm_id, adv.nm_id) AS nm_id,
        COALESCE(cr.date_of_period, adv.date) AS "date",
        p.vendor_code,
        
        -- Из cr_daily_stats
        cr.orders_count,
        cr.orders_sum_rub,
        cr.open_card_count,
        cr.add_to_cart_count,
        cr.add_to_cart_percent,
        cr.cart_to_order_percent,
        cr.order_price,
        
        -- Из adv_params
        adv.views,
        adv.clicks,
        adv.sum,
        adv.cpc,
        adv.cpm,
        adv.ctr
        
    FROM products p
    LEFT JOIN cr_daily_stats cr ON p.nm_id = cr.nm_id
    LEFT JOIN adv_params adv ON p.nm_id = adv.nm_id 
        AND cr.date_of_period = adv.date
    WHERE COALESCE(cr.date_of_period, adv.date) BETWEEN '{start_date}' AND '{end_date}'
    AND COALESCE(cr.nm_id, adv.nm_id) IS NOT NULL
    ORDER BY COALESCE(cr.date_of_period, adv.date), COALESCE(cr.nm_id, adv.nm_id);
    """
    
    db_data_map = {}
    conn = None
    try:
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        logging.info("📡 Выполняю SQL-запрос для LEVEL 1...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        
        for row in cursor.fetchall():
            row_dict = {}
            nm_id = None
            row_date = None
            
            for i, value in enumerate(row):
                col_name = columns[i]
                
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                
                if col_name == 'nm_id':
                    nm_id = value
                elif col_name == 'date':
                    row_date = value
                else:
                    row_dict[col_name] = value if value is not None else 0
            
            if nm_id and row_date:
                db_data_map[(nm_id, row_date)] = row_dict
        
        logging.info(f"✅ LEVEL 1: Извлечено {len(db_data_map)} записей")
        return db_data_map
        
    except Exception as e:
        logging.error(f"❌ Ошибка при fetch LEVEL 1: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def fetch_raw_data_for_aggregation(
    date_range: Tuple[date, date],
    db_url: str,
    group_by_field: str  # "category_wb", "imt_id", или "store"
) -> Dict[Tuple[Any, date], Dict[str, Any]]:
    """
    Fetch сырых данных для LEVEL 2, 3, 4 (с GROUP BY и агрегацией).
    
    Args:
        group_by_field: "category_wb" (L2), "imt_id" (L3), или "store" (L4)
    
    Returns:
        {(key_id, date): {attr: value}}
    """
    start_date, end_date = date_range
    
    if group_by_field == "category_wb":
        logging.info("🔍 LEVEL 2: Fetch данных по категориям...")
        key_field = "p.category_wb"
        key_alias = "key_id"
    elif group_by_field == "imt_id":
        logging.info("🔍 LEVEL 3: Fetch данных по склейкам...")
        key_field = "p.imt_id"
        key_alias = "key_id"
    elif group_by_field == "store":
        logging.info("🔍 LEVEL 4: Fetch данных по магазину...")
        key_field = "'Магазин'"
        key_alias = "key_id"
    else:
        logging.error(f"❌ Неверное значение group_by_field: {group_by_field}")
        return {}
    
    query = f"""
    SELECT
        {key_field} AS {key_alias},
        COALESCE(cr.date_of_period, adv.date) AS "date",
        
        -- Агрегированные метрики
        SUM(COALESCE(cr.orders_count, 0)) AS orders_count,
        SUM(COALESCE(cr.orders_sum_rub, 0)) AS orders_sum_rub,
        SUM(COALESCE(cr.open_card_count, 0)) AS open_card_count,
        SUM(COALESCE(cr.add_to_cart_count, 0)) AS add_to_cart_count,
        AVG(CASE WHEN cr.add_to_cart_percent IS NOT NULL THEN cr.add_to_cart_percent ELSE 0 END) AS add_to_cart_percent,
        AVG(CASE WHEN cr.cart_to_order_percent IS NOT NULL THEN cr.cart_to_order_percent ELSE 0 END) AS cart_to_order_percent,
        AVG(COALESCE(cr.order_price, 0)) AS order_price,
        
        SUM(COALESCE(adv.views, 0)) AS views,
        SUM(COALESCE(adv.clicks, 0)) AS clicks,
        SUM(COALESCE(adv.sum, 0)) AS sum,
        
        -- Вычисляемые метрики
        CASE 
            WHEN SUM(COALESCE(adv.clicks, 0)) > 0 
            THEN SUM(COALESCE(adv.sum, 0)) / SUM(COALESCE(adv.clicks, 0))
            ELSE 0 
        END AS cpc,
        
        CASE 
            WHEN SUM(COALESCE(adv.views, 0)) > 0 
            THEN (SUM(COALESCE(adv.sum, 0)) / SUM(COALESCE(adv.views, 0))) * 1000
            ELSE 0 
        END AS cpm,
        
        CASE 
            WHEN SUM(COALESCE(adv.views, 0)) > 0 
            THEN (SUM(COALESCE(adv.clicks, 0))::NUMERIC / SUM(COALESCE(adv.views, 0))) * 100
            ELSE 0 
        END AS ctr
        
    FROM products p
    LEFT JOIN cr_daily_stats cr ON p.nm_id = cr.nm_id
    LEFT JOIN adv_params adv ON p.nm_id = adv.nm_id 
        AND cr.date_of_period = adv.date
    WHERE COALESCE(cr.date_of_period, adv.date) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY {key_alias}, COALESCE(cr.date_of_period, adv.date)
    ORDER BY {key_alias}, "date";
    """
    
    db_data_map = {}
    conn = None
    try:
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        logging.info(f"📡 Выполняю SQL-запрос для {group_by_field}...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        
        for row in cursor.fetchall():
            row_dict = {}
            key_id = None
            row_date = None
            
            for i, value in enumerate(row):
                col_name = columns[i]
                
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                
                if col_name == 'key_id':
                    key_id = value
                elif col_name == 'date':
                    row_date = value
                else:
                    row_dict[col_name] = value if value is not None else 0
            
            if key_id and row_date:
                db_data_map[(key_id, row_date)] = row_dict
        
        logging.info(f"✅ {group_by_field}: Извлечено {len(db_data_map)} записей")
        return db_data_map
        
    except Exception as e:
        logging.error(f"❌ Ошибка при fetch {group_by_field}: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def fetch_current_sheet_data(service, spreadsheet_id: str, sheet_name: str) -> List[List[Any]]:
    """
    Fetch текущих данных из Google Sheets для backup.
    
    Args:
        service: Google Sheets API service
        spreadsheet_id: ID таблицы
        sheet_name: Название листа
    
    Returns:
        Список списков (строк)
    """
    logging.info(f"📖 Fetch текущих данных из листа '{sheet_name}'...")
    
    try:
        sheet_range = f"'{sheet_name}'!A:Z"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_range
        ).execute()
        
        values = result.get('values', [])
        
        if values:
            logging.info(f"✅ Fetch завершен: {len(values)} строк (включая хедеры)")
            return values
        else:
            logging.warning(f"⚠️  Лист '{sheet_name}' пуст")
            return []
    
    except Exception as e:
        logging.error(f"❌ Ошибка при fetch данных из Google: {e}")
        return []


def fetch_headers_from_sheet(service, spreadsheet_id: str, sheet_name: str) -> List[str]:
    """
    Fetch хедеров из Google Sheets.
    
    Returns:
        Список названий хедеров
    """
    logging.info(f"📋 Fetch хедеров из листа '{sheet_name}'...")
    
    try:
        sheet_range = f"'{sheet_name}'!A1:Z1"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_range
        ).execute()
        
        values = result.get('values', [[]])
        headers = values[0] if values else []
        
        logging.info(f"✅ Fetch хедеров завершен: {len(headers)} колонок")
        return headers
    
    except Exception as e:
        logging.error(f"❌ Ошибка при fetch хедеров: {e}")
        return []

