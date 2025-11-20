"""
Модуль для агрегации данных по предметам (category_wb) для РНП.
Суммирует метрики всех артикулов с одинаковой категорией.
"""
import logging
import decimal
import psycopg2
from typing import Dict, Any, Set
from datetime import date


def _round_percent(value: float, decimals: int = 2) -> float:
    """Вспомогательная функция для округления процентов."""
    if value is None or value == 0:
        return 0
    return round(value, decimals)


def get_db_connection(db_url: str):
    """Инициализирует и возвращает подключение к базе данных."""
    conn = psycopg2.connect(db_url)
    return conn


def get_data_by_category(date_range: Set[date], db_url: str) -> Dict[tuple, Dict[str, Any]]:
    """
    Извлекает агрегированные данные из БД по предметам (category_wb).
    
    Args:
        date_range: Множество дат для выборки
        db_url: URL подключения к БД
        
    Returns:
        Словарь вида {(category_wb, date): {attr: value}}
    """
    logging.info(f"Запрос агрегированных данных по предметам для {len(date_range)} дат...")
    logging.info(f"Диапазон дат: с {min(date_range).strftime('%Y-%m-%d')} по {max(date_range).strftime('%Y-%m-%d')}")
    
    start_date = min(date_range).strftime('%Y-%m-%d')
    end_date = max(date_range).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT
        p.category_wb AS key_id,
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
    GROUP BY p.category_wb, COALESCE(cr.date_of_period, adv.date)
    ORDER BY p.category_wb, "date";
    """
    
    db_data_map = {}
    conn = None
    try:
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        logging.info(f"Выполняю SQL-запрос агрегации по category_wb...")
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
                # Вычисляем ДРР = (Расход на рекламу / Сумма заказов) * 100
                adv_spend = row_dict.get("Расход на рекламу", 0)
                orders_sum = row_dict.get("Сумма заказов", 0)
                drr = (adv_spend / orders_sum * 100) if orders_sum > 0 else 0
                row_dict["ДРР"] = _round_percent(drr, 2)
                
                # Вычисляем CPM - округляем до целого
                cpm_val = row_dict.get("CPM", 0)
                if isinstance(cpm_val, (int, float)) and cpm_val != 0:
                    row_dict["CPM"] = round(cpm_val)
                
                # CPC - округляем до двух знаков после запятой
                cpc_val = row_dict.get("CPC", 0)
                if isinstance(cpc_val, (int, float)) and cpc_val != 0:
                    row_dict["CPC"] = round(cpc_val, 2)
                
                db_data_map[(key_id, row_date)] = row_dict
        
        logging.info(f"✅ Из БД извлечено {len(db_data_map)} агрегированных записей по предметам.")
        
        # Дополнительная диагностика
        if len(db_data_map) == 0:
            logging.warning(f"⚠️ ВНИМАНИЕ: Не найдено ни одной записи для указанного диапазона дат.")
        else:
            unique_categories = len(set([key[0] for key in db_data_map.keys()]))
            logging.info(f"Найдено уникальных предметов: {unique_categories}")
        
        return db_data_map
        
    except Exception as e:
        logging.error(f"❌ Ошибка при работе с БД: {e}")
        return None
    finally:
        if conn:
            conn.close()

