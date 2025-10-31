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

def read_all_data_from_gs():
    """Читает все данные с листа 'База данных'."""
    print("Чтение данных из Google Sheets...")
    try:
        service = get_google_sheets_service()
        sheet_name_for_range = f"'{SHEET_NAME}'" if ' ' in SHEET_NAME else SHEET_NAME
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=sheet_name_for_range
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 1:
            print("⚠️ В таблице нет данных или заголовков.")
            return [], {}

        headers = values[0]
        data = values[1:]
        
        data_map = {}
        articul_idx = headers.index("Артикул")
        date_idx = headers.index("Дата")

        for i, row in enumerate(data):
            try:
                key = (str(row[articul_idx]), str(row[date_idx]))
                row_data = {headers[j]: val for j, val in enumerate(row)}
                row_data['__row_index'] = i + 2
                data_map[key] = row_data
            except IndexError:
                continue
                
        print(f"✅ Прочитано {len(data_map)} строк из Google Sheets.")
        return headers, data_map
    except Exception as e:
        print(f"❌ Ошибка при чтении из Google Sheets: {e}")
        return [], {}


def main():
    """
    Главная функция для выполнения умного обновления (upsert).
    """
    print("🚀 Запуск умного обновления данных в Google Sheets...")

    gs_headers, gs_data_map = read_all_data_from_gs()
    if not gs_headers:
        return

    db_data = get_data_from_db()
    if not db_data:
        return
        
    print("🔬 Сравнение данных и подготовка изменений...")
    
    updates_to_make = []
    new_rows_to_add = []
    
    db_data_map = {
        (str(row["Артикул"]), str(row["Дата"])): row 
        for row in db_data
    }

    db_keys = set(db_data_map.keys())
    gs_keys = set(gs_data_map.keys())

    # Находим строки для ОБНОВЛЕНИЯ
    common_keys = db_keys.intersection(gs_keys)
    for key in common_keys:
        db_row = db_data_map[key]
        gs_row = gs_data_map[key]
        
        is_different = False
        for header in gs_headers:
            db_val = str(db_row.get(header, ''))
            gs_val = str(gs_row.get(header, ''))
            if db_val != gs_val:
                is_different = True
                break
        
        if is_different:
            row_index = gs_row['__row_index']
            ordered_row = [db_row.get(h, "") for h in gs_headers]
            updates_to_make.append({
                "range": f"'{SHEET_NAME}'!A{row_index}",
                "values": [ordered_row]
            })

    # Находим строки для ДОБАВЛЕНИЯ
    new_keys = db_keys - gs_keys
    for key in new_keys:
        db_row = db_data_map[key]
        ordered_row = [db_row.get(h, "") for h in gs_headers]
        new_rows_to_add.append(ordered_row)

    print(f"🔍 Найдено: {len(updates_to_make)} строк для обновления, {len(new_rows_to_add)} новых строк.")

    if not updates_to_make and not new_rows_to_add:
        print("✅ Данные в Google Sheets актуальны. Обновление не требуется.")
        return

    try:
        service = get_google_sheets_service()
        
        if updates_to_make:
            print(f"🔄 Обновление {len(updates_to_make)} существующих строк...")
            update_body = {'valueInputOption': 'USER_ENTERED', 'data': updates_to_make}
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=update_body
            ).execute()
            print("✅ Строки успешно обновлены.")
        
        if new_rows_to_add:
            print(f"➕ Добавление {len(new_rows_to_add)} новых строк...")
            append_body = {'values': new_rows_to_add}
            service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_NAME}'!A1",
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=append_body
            ).execute()
            print("✅ Новые строки успешно добавлены.")

    except Exception as e:
        print(f"❌ Ошибка при обновлении Google Sheets: {e}")


if __name__ == "__main__":
    main()
