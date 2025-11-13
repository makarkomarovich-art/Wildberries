"""
Модуль для агрегации данных по номерам склеек (imt_id) для РНП.
Суммирует метрики всех артикулов с одинаковым imt_id.
"""
import logging
import decimal
import psycopg2
from typing import Dict, Any, Set
from datetime import date


def get_db_connection(db_url: str):
    """Инициализирует и возвращает подключение к базе данных."""
    conn = psycopg2.connect(db_url)
    return conn


def get_data_by_imt(date_range: Set[date], db_url: str) -> Dict[tuple, Dict[str, Any]]:
    """
    Извлекает агрегированные данные из БД по номерам склеек (imt_id).
    
    Args:
        date_range: Множество дат для выборки
        db_url: URL подключения к БД
        
    Returns:
        Словарь вида {(imt_id, date): {attr: value}}
    """
    logging.info(f"Запрос агрегированных данных по склейкам для {len(date_range)} дат...")
    logging.info(f"Диапазон дат: с {min(date_range).strftime('%Y-%m-%d')} по {max(date_range).strftime('%Y-%m-%d')}")
    
    start_date = min(date_range).strftime('%Y-%m-%d')
    end_date = max(date_range).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT
        p.imt_id AS key_id,
        COALESCE(cr.date_of_period, adv.date) AS "date",
        
        -- Агрегированные метрики
        SUM(cr.orders_count) AS "Заказы",
        SUM(cr.orders_sum_rub) AS "Сумма заказов",
        SUM(adv.views) AS "Рекламные просмотры",
        SUM(adv.clicks) AS "Рекламные клики",
        SUM(adv.sum) AS "Расход на рекламу",
        -- Правильный расчет CPC и CPM от агрегированных данных
        CASE 
            WHEN SUM(adv.clicks) > 0 
            THEN SUM(adv.sum) / SUM(adv.clicks)
            ELSE 0 
        END AS "CPC",
        CASE 
            WHEN SUM(adv.views) > 0 
            THEN (SUM(adv.sum) / SUM(adv.views)) * 1000
            ELSE 0 
        END AS "CPM"
        
    FROM products p
    LEFT JOIN cr_daily_stats cr ON p.nm_id = cr.nm_id
    LEFT JOIN adv_params adv ON p.nm_id = adv.nm_id 
        AND cr.date_of_period = adv.date
    WHERE COALESCE(cr.date_of_period, adv.date) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY p.imt_id, COALESCE(cr.date_of_period, adv.date)
    ORDER BY p.imt_id, "date";
    """
    
    db_data_map = {}
    conn = None
    try:
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        logging.info(f"Выполняю SQL-запрос агрегации по imt_id...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        
        for row in cursor.fetchall():
            row_dict = {}
            key_id = None
            row_date = None
            
            for i, value in enumerate(row):
                col_name = columns[i]
                
                # Конвертация Decimal в float
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
        
        logging.info(f"✅ Из БД извлечено {len(db_data_map)} агрегированных записей по склейкам.")
        
        # Дополнительная диагностика
        if len(db_data_map) == 0:
            logging.warning(f"⚠️ ВНИМАНИЕ: Не найдено ни одной записи для указанного диапазона дат.")
        else:
            unique_imt_ids = len(set([key[0] for key in db_data_map.keys()]))
            logging.info(f"Найдено уникальных склеек: {unique_imt_ids}")
        
        return db_data_map
        
    except Exception as e:
        logging.error(f"❌ Ошибка при работе с БД: {e}")
        return None
    finally:
        if conn:
            conn.close()

