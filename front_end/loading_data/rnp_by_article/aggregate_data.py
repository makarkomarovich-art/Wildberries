"""
Модуль для получения данных из БД для РНП по артикулам.
Извлекает данные для конкретных артикулов (nm_id) без агрегации.
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


def get_data_by_articles(article_ids: Set[int], date_range: Set[date], db_url: str) -> Dict[tuple, Dict[str, Any]]:
    """
    Извлекает данные из БД для заданных артикулов и диапазона дат.
    
    Args:
        article_ids: Множество nm_id артикулов
        date_range: Множество дат для выборки
        db_url: URL подключения к БД
        
    Returns:
        Словарь вида {(nm_id, date): {attr: value}}
    """
    logging.info(f"Запрос данных из БД для {len(article_ids)} артикулов и {len(date_range)} дат...")
    logging.info(f"Ищем артикулы (nm_id): {list(article_ids)[:10]}{'...' if len(article_ids) > 10 else ''}")
    logging.info(f"Диапазон дат: с {min(date_range).strftime('%Y-%m-%d')} по {max(date_range).strftime('%Y-%m-%d')}")
    
    start_date = min(date_range).strftime('%Y-%m-%d')
    end_date = max(date_range).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT
        COALESCE(cr.nm_id, adv.nm_id) AS nm_id,
        COALESCE(cr.date_of_period, adv.date) AS "date",
        
        -- Метрики из cr_daily_stats
        cr.open_card_count AS "Клики общие",
        cr.add_to_cart_count AS "В корзину",
        cr.orders_count AS "Заказы",
        cr.orders_sum_rub AS "Сумма заказов",
        cr.add_to_cart_percent AS "Конверсия в корзину",
        cr.cart_to_order_percent AS "Конверсия в заказ",
        cr.order_price AS "Цена одного заказа",
        
        -- Метрики из adv_params
        adv.views AS "Рекламные просмотры",
        adv.clicks AS "Рекламные клики",
        adv.sum AS "Расход на рекламу",
        adv.cpc AS "CPC",
        adv.cpm AS "CPM",
        adv.ctr AS "CTR"
        
    FROM cr_daily_stats cr
    FULL OUTER JOIN adv_params adv 
        ON cr.nm_id = adv.nm_id AND cr.date_of_period = adv.date
    WHERE 
        COALESCE(cr.nm_id, adv.nm_id) IN ({','.join(map(str, article_ids))})
    AND 
        COALESCE(cr.date_of_period, adv.date) BETWEEN '{start_date}' AND '{end_date}';
    """
    
    db_data_map = {}
    conn = None
    try:
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        logging.info(f"Выполняю SQL-запрос...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        
        for row in cursor.fetchall():
            row_dict = {}
            nm_id = None
            row_date = None
            
            for i, value in enumerate(row):
                col_name = columns[i]
                
                # Конвертация Decimal в float
                if isinstance(value, decimal.Decimal):
                    value = float(value)
                
                if col_name == 'nm_id':
                    nm_id = value
                elif col_name == 'date':
                    row_date = value
                else:
                    row_dict[col_name] = value if value is not None else 0

            if nm_id and row_date:
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
                
                db_data_map[(nm_id, row_date)] = row_dict
        
        logging.info(f"✅ Из БД извлечено {len(db_data_map)} записей.")
        
        # Дополнительная диагностика: проверяем, есть ли данные для этих артикулов вообще
        if len(db_data_map) == 0:
            logging.warning(f"⚠️ ВНИМАНИЕ: Не найдено ни одной записи в БД для указанных артикулов.")
            logging.warning(f"Проверяю наличие данных по vendor_code...")
            # Проверяем, может быть данные есть по vendor_code?
            check_query = f"""
            SELECT DISTINCT cr.vendor_code, cr.nm_id 
            FROM cr_daily_stats cr 
            WHERE cr.vendor_code IN (
                SELECT vendor_code FROM products WHERE nm_id IN ({','.join(map(str, article_ids))})
            )
            LIMIT 5;
            """
            cursor.execute(check_query)
            vendor_check = cursor.fetchall()
            if vendor_check:
                logging.warning(f"Найдены данные по vendor_code для этих nm_id: {vendor_check}")
            else:
                logging.warning(f"Данных по vendor_code тоже не найдено.")
        
        return db_data_map
        
    except Exception as e:
        logging.error(f"❌ Ошибка при работе с БД: {e}")
        return None
    finally:
        if conn:
            conn.close()

