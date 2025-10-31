import sys
from pathlib import Path
from datetime import datetime, timedelta
import decimal

# Добавляем родительскую директорию в sys.path, чтобы найти processing_keys
# Это позволит запускать скрипт напрямую
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))
sys.path.append(str(Path(__file__).resolve().parents[1]))

from processing_keys import (
    get_db_connection, 
    get_google_sheets_service, 
    SPREADSHEET_ID, 
    SHEET_NAME
)


def get_data_from_db():
    """
    Извлекает и объединяет данные из таблиц cr_daily_stats и adv_params.
    """
    print("Извлечение данных из базы данных...")
    
    # Дата 3 месяца назад от сегодняшнего дня
    date_threshold = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT
        COALESCE(cr.nm_id, adv.nm_id) AS "Артикул",
        COALESCE(cr.vendor_code, adv.vendor_code) AS "Артикул продавца",
        TO_CHAR(COALESCE(cr.date_of_period, adv.date), 'YYYY-MM-DD') AS "Дата",
        
        -- Метрики из cr_daily_stats
        COALESCE(cr.open_card_count, 0) AS "Клики общие",
        COALESCE(cr.add_to_cart_count, 0) AS "В корзину",
        COALESCE(cr.orders_count, 0) AS "Заказы",
        COALESCE(cr.orders_sum_rub, 0) AS "Сумма заказов",
        COALESCE(cr.stocks_wb, 0) AS "Остатки WB",
        COALESCE(cr.add_to_cart_percent, 0) AS "Конверсия в корзину",
        COALESCE(cr.cart_to_order_percent, 0) AS "Конверсия в заказ",
        
        -- Метрики из adv_params
        COALESCE(adv.views, 0) AS "Рекламные просмотры",
        COALESCE(adv.clicks, 0) AS "Рекламные клики",
        COALESCE(adv.sum, 0) AS "Расход на рекламу",
        COALESCE(adv.cpc, 0) AS "CPC",
        COALESCE(adv.cpm, 0) AS "CPM",
        COALESCE(adv.ctr, 0) AS "CTR",
        COALESCE(adv.orders_sum, 0) AS "Сумма заказов со склейки"
        
    FROM cr_daily_stats cr
    FULL OUTER JOIN adv_params adv 
        ON cr.nm_id = adv.nm_id AND cr.date_of_period = adv.date
    WHERE 
        COALESCE(cr.date_of_period, adv.date) >= '{date_threshold}';
    """
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        
        data = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, value in enumerate(row):
                if isinstance(value, decimal.Decimal):
                    row_dict[columns[i]] = float(value)
                else:
                    row_dict[columns[i]] = value
            data.append(row_dict)
        
        print(f"✅ Извлечено {len(data)} строк из БД.")
        return data
        
    except Exception as e:
        print(f"❌ Ошибка при работе с БД: {e}")
        return None
    finally:
        if conn:
            conn.close()

def write_data_to_gs(data):
    """
    Очищает данные под заголовками и записывает новые,
    сопоставляя их с порядком заголовков в таблице.
    """
    if not data:
        print("Нет данных для записи.")
        return

    print("Запись данных в Google Sheets...")
    
    try:
        service = get_google_sheets_service()
        
        # 1. Чтение заголовков из первой строки таблицы
        print("Чтение заголовков из Google Sheets...")
        sheet_name_for_range = f"'{SHEET_NAME}'" if ' ' in SHEET_NAME else SHEET_NAME
        range_headers = f"{sheet_name_for_range}!1:1"
        
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_headers
        ).execute()
        
        gs_headers = result.get('values', [[]])[0]
        if not gs_headers:
            print("❌ Не удалось прочитать заголовки из таблицы. Убедитесь, что лист не пуст и первая строка содержит заголовки.")
            return
        print(f"✅ Заголовки прочитаны.")

        # 2. Очистка данных (все, кроме первой строки)
        print(f"Очистка данных на листе '{SHEET_NAME}' (начиная со строки 2)...")
        range_to_clear = f"{sheet_name_for_range}!A2:Z" # Достаточный диапазон для очистки
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=range_to_clear
        ).execute()
        print("✅ Старые данные очищены.")

        # 3. Подготовка данных для записи в соответствии с порядком заголовков в Google-таблице
        print("Подготовка и сортировка данных для записи...")
        values_to_write = []
        for row_dict in data:
            ordered_row = [row_dict.get(header, 0) for header in gs_headers]
            values_to_write.append(ordered_row)
        
        if not values_to_write:
            print("ℹ️ После обработки не осталось данных для записи.")
            return

        body = {
            'values': values_to_write
        }
        
        # 4. Запись данных, начиная со второй строки (A2)
        print(f"Запись {len(values_to_write)} строк...")
        range_to_write = f"{sheet_name_for_range}!A2"
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=range_to_write,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        
        print("✅ Данные успешно записаны в Google Sheets!")
        
    except Exception as e:
        print(f"❌ Ошибка при записи в Google Sheets: {e}")


def main():
    """
    Главная функция для запуска процесса.
    """
    data = get_data_from_db()
    
    if data:
        write_data_to_gs(data)


if __name__ == "__main__":
    main()
