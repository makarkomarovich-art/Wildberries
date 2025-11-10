import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для импорта api_keys
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from api_keys import (
    RNP_REPORT_ID,
    SHEET_NAMES,
    GOOGLE_CREDENTIALS_INFO,
    SUPABASE_DB_URL_LOCAL as SUPABASE_DB_URL,
)

import logging
from datetime import datetime, timedelta
import decimal

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import psycopg2


# ==================================
#        НАСТРОЙКИ ЛОГИРОВАНИЯ
# ==================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)


# ==================================
#        НАСТРОЙКИ ОТЧЕТА "РНП"
# ==================================
UPDATE_TIMESTAMP_CELL = 'E2'

# Названия колонок для автоматического поиска
ARTICLE_HEADER_NAME = "Артикул"
ATTRIBUTES_HEADER_NAME = "Атрибуты"
FIRST_DATE_COLUMN_INDEX = 6 # E - Индекс первой колонки с датой (нумерация с 1)

# Структура блока одного товара
ATTRIBUTES_PER_ITEM = 16
ATTRIBUTE_ORDER = [
    "Заказы", "Сумма заказов", "Расход на рекламу", "ДРР",
    "Клики общие", "В корзину", "Конверсия в корзину", "Конверсия в заказ",
    "CPM", "Рекламные просмотры", "Рекламные клики", "CTR", "CPC",
    "Остатки WB", "Цена одного заказа", "Журнал изменений",
]

def get_google_sheets_service():
    """Инициализирует и возвращает сервис для работы с Google Sheets API."""
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    if not GOOGLE_CREDENTIALS_INFO:
        raise ValueError("Учетные данные GOOGLE_CREDENTIALS_INFO не найдены в api_keys.py")

    credentials = Credentials.from_service_account_info(GOOGLE_CREDENTIALS_INFO, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)
    return service

def get_db_connection():
    """Инициализирует и возвращает подключение к базе данных."""
    conn = psycopg2.connect(SUPABASE_DB_URL)
    return conn


def read_and_validate_structure(service, spreadsheet_id, sheet_name):
    """
    Читает структуру листа "РНП", находит ключевые колонки,
    собирает информацию об артикулах и валидирует их структуру.
    """
    logging.info(f"Начало чтения и валидации структуры листа '{sheet_name}'...")

    sheet_range = f"'{sheet_name}'!A:Z"
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_range).execute()
    values = result.get('values', [])
    if not values:
        logging.error("Лист не найден или пуст.")
        return None, None

    header_row = values[0]
    try:
        article_col_idx = header_row.index(ARTICLE_HEADER_NAME)
        attributes_col_idx = header_row.index(ATTRIBUTES_HEADER_NAME)
    except ValueError as e:
        logging.error(f"Критическая ошибка: Не найдена одна из обязательных колонок: {e}. Выполнение прервано.")
        return None, None
        
    logging.info(f"Колонка '{ARTICLE_HEADER_NAME}' найдена в столбце: {article_col_idx + 1}, '{ATTRIBUTES_HEADER_NAME}' в столбце: {attributes_col_idx + 1}")
    logging.info(f"Схема для валидации (эталонный порядок атрибутов): {ATTRIBUTE_ORDER}")

    validated_articles = {}
    total_found = 0
    row_idx = 1
    while row_idx < len(values):
        article_cell_value = values[row_idx][article_col_idx] if len(values[row_idx]) > article_col_idx else None
        
        if not article_cell_value:
            row_idx += 1
            continue
        
        total_found += 1
        
        try:
            nm_id = int(article_cell_value)
        except ValueError:
            row_idx += 1
            continue

        if row_idx + ATTRIBUTES_PER_ITEM > len(values):
            row_idx += 1
            continue

        current_attributes = [
            values[row_idx + i][attributes_col_idx] if len(values[row_idx + i]) > attributes_col_idx else None
            for i in range(ATTRIBUTES_PER_ITEM)
        ]
        if current_attributes != ATTRIBUTE_ORDER:
            row_idx += ATTRIBUTES_PER_ITEM
            continue
        
        if nm_id in validated_articles:
            logging.warning(f"ВНИМАНИЕ: Артикул {nm_id} (строка {row_idx + 1}) является дубликатом и перезапишет предыдущие данные.")

        validated_articles[nm_id] = {
            "start_row": row_idx + 1,
            "attributes": {attr: row_idx + 1 + i for i, attr in enumerate(ATTRIBUTE_ORDER)}
        }
        row_idx += ATTRIBUTES_PER_ITEM

    logging.info(f"Найдено кандидатов в колонке артикулов: {total_found}")
    logging.info(f"Количество уникальных артикулов, прошедших валидацию: {len(validated_articles)}")
    if not validated_articles:
        logging.warning("Не найдено ни одного валидного артикула. Дальнейшая работа невозможна.")
        return None, None

    return validated_articles, values


def get_and_validate_dates(service, spreadsheet_id, sheet_name, header_row):
    """
    Сканирует даты в заголовке, проверяет их до сегодняшнего дня
    и готовит запрос на добавление завтрашней даты, если её нет.
    """
    logging.info("Начало сканирования и валидации дат...")
    logging.info(f"Поиск дат начинается с колонки: {FIRST_DATE_COLUMN_INDEX}")
    
    today = datetime.now().date()
    tomorrow = today + timedelta(days=1)
    
    date_columns = {}
    today_found = False
    tomorrow_found = False
    
    # 1. Сканируем даты
    col_idx = FIRST_DATE_COLUMN_INDEX - 1
    while col_idx < len(header_row):
        try:
            cell_date = datetime.strptime(header_row[col_idx], '%d.%m.%y').date()
            date_columns[cell_date] = col_idx + 1

            if cell_date == today:
                today_found = True
                # Проверяем, есть ли следующая колонка для "завтра"
                if col_idx + 1 < len(header_row):
                    next_cell_date_str = header_row[col_idx + 1]
                    if next_cell_date_str:
                         next_cell_date = datetime.strptime(next_cell_date_str, '%d.%m.%y').date()
                         if next_cell_date == tomorrow:
                             tomorrow_found = True
                break # Останавливаемся на сегодня
            
            col_idx += 1
        except (ValueError, IndexError):
            # Дошли до конца дат
            break
            
    # 2. Проверка диапазона
    if not today_found:
        logging.error(f"❌ ОШИБКА: Сегодняшняя дата ({today.strftime('%d.%m.%Y')}) не найдена в заголовках. Пожалуйста, проверьте даты в таблице. Выполнение прервано.")
        return None, None

    logging.info(f"Найдено дат для обработки: {len(date_columns)}. Диапазон: с {min(date_columns.keys()).strftime('%d.%m.%Y')} по {max(date_columns.keys()).strftime('%d.%m.%Y')}")

    # 3. Добавление "завтрашнего дня"
    add_column_request = None
    if not tomorrow_found:
        logging.info(f"Завтрашняя дата ({tomorrow.strftime('%d.%m.%Y')}) не найдена. Подготовка к добавлению нового столбца...")
        
        # Находим ID листа для запроса
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None
        for s in sheet_metadata.get('sheets', ''):
            if s.get('properties', {}).get('title', '') == sheet_name:
                sheet_id = s.get('properties', {}).get('sheetId', '')
                break
        
        if sheet_id is not None:
            last_date_col_index = max(date_columns.values())
            add_column_request = {
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "length": 1
                }
            }
            # Этот запрос добавит пустую колонку. Нам нужно будет еще вписать в нее дату.
            # Пока просто вернем сам факт необходимости добавления.
            # Для простоты, мы добавим колонку, но запишем дату в нее уже в общем батче.
            logging.info("Запрос на добавление столбца для завтрашней даты сформирован.")
            # Пока не выполняем, а просто возвращаем для batchUpdate
    else:
        logging.info(f"Завтрашняя дата ({tomorrow.strftime('%d.%m.%Y')}) уже существует в таблице.")

    return date_columns, add_column_request


def get_data_from_db(article_ids, date_range):
    """
    Извлекает данные из БД для заданных артикулов и диапазона дат.
    Возвращает данные в виде словаря для быстрого доступа.
    """
    logging.info(f"Запрос данных из БД для {len(article_ids)} артикулов и {len(date_range)} дат...")
    
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
        cr.stocks_wb AS "Остатки WB",
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
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
                db_data_map[(nm_id, row_date)] = row_dict
        
        logging.info(f"✅ Из БД извлечено {len(db_data_map)} записей.")
        return db_data_map
        
    except Exception as e:
        logging.error(f"❌ Ошибка при работе с БД: {e}")
        return None
    finally:
        if conn:
            conn.close()


def compare_and_update(service, spreadsheet_id, sheet_name, sheet_data, validated_articles, date_columns, db_data, add_column_req):
    """
    Сравнивает данные из GS и БД, формирует и отправляет batch-запрос на обновление.
    """
    logging.info("Начало сравнения данных и подготовки batch-запроса...")
    
    update_requests = []
    inserted_cells_count = 0
    updated_cells_count = 0
    
    # Добавляем запрос на создание новой колонки, если он есть
    if add_column_req:
        update_requests.append(add_column_req)
        # И сразу добавляем запрос на запись даты в заголовок новой колонки
        tomorrow = datetime.now().date() + timedelta(days=1)
        last_date_col_index = max(date_columns.values())
        
        update_requests.append({
            "updateCells": {
                "rows": [{
                    "values": [{
                        "userEnteredValue": {"stringValue": tomorrow.strftime('%d.%m.%Y')},
                        "userEnteredFormat": {"horizontalAlignment": "CENTER"}
                    }]
                }],
                "start": {"sheetId": add_column_req['appendDimension']['sheetId'], "rowIndex": 0, "columnIndex": last_date_col_index},
                "fields": "userEnteredValue,userEnteredFormat"
            }
        })
        logging.info(f"Запрос на запись даты {tomorrow.strftime('%d.%m.%Y')} в новый столбец добавлен в батч.")

    
    checkable_attributes_count = len([attr for attr in ATTRIBUTE_ORDER if attr not in ["ДРР", "Журнал изменений"]])
    total_cells_to_check = len(validated_articles) * checkable_attributes_count * len(date_columns)
    logging.info(f"Всего ячеек для проверки (артикулы * атрибуты * даты): {total_cells_to_check}")

    batch_update_values_data = []

    for nm_id, article_info in validated_articles.items():
        for attr_name, row_num in article_info['attributes'].items():
            if attr_name == "ДРР" or attr_name == "Журнал изменений":
                continue # Пропускаем вычисляемые и нетрогаемые поля

            for date_obj, col_num in date_columns.items():
                
                # Получаем старое значение из GS
                try:
                    old_value_str = sheet_data[row_num - 1][col_num - 1]
                except IndexError:
                    old_value_str = "" # Ячейка пуста

                is_insertion = old_value_str.strip() == ""

                # Получаем новое значение из БД
                db_record = db_data.get((nm_id, date_obj))
                new_value = db_record.get(attr_name, 0) if db_record else 0

                # Приводим типы для сравнения
                try:
                    # Попытка преобразовать старое значение в число, если это возможно
                    if isinstance(old_value_str, str) and '%' in old_value_str:
                         old_value = float(old_value_str.replace('%', '').replace(',', '.')) / 100
                    elif isinstance(old_value_str, str) and old_value_str.strip():
                        old_value = float(old_value_str.replace(',', '.'))
                    elif old_value_str == "":
                        old_value = 0
                    else:
                        old_value = float(old_value_str)
                except (ValueError, TypeError):
                    old_value = old_value_str # Оставляем строкой, если не конвертируется

                # Сравниваем, избегая проблем с float
                if not isinstance(old_value, str) and not isinstance(new_value, str):
                    are_different = not abs(float(old_value) - float(new_value)) < 1e-9
                else:
                    are_different = old_value != new_value

                if are_different:
                    if is_insertion:
                        inserted_cells_count += 1
                    else:
                        updated_cells_count += 1
                    
                    batch_update_values_data.append({
                        'range': f"'{sheet_name}'!R{row_num}C{col_num}",
                        'values': [[new_value]]
                    })

    # Добавляем ячейку с временем последнего обновления
    timestamp_str = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    batch_update_values_data.append({
        'range': f"'{sheet_name}'!{UPDATE_TIMESTAMP_CELL}",
        'values': [[f"Последнее обновление: {timestamp_str}"]]
    })

    logging.info(f"Найдено ячеек для вставки (inserted): {inserted_cells_count}")
    logging.info(f"Найдено ячеек для обновления (updated): {updated_cells_count}")
    total_changes = updated_cells_count + inserted_cells_count
    logging.info(f"Общее количество ячеек в batch-запросе (включая метку времени): {total_changes + 1}")

    if not batch_update_values_data and not update_requests:
        # Эта ветка теперь вряд ли будет достигнута, т.к. метка времени всегда есть
        logging.info("Нет данных для обновления. Все значения актуальны.")
        return True

    try:
        # Обновление неструктурных изменений (добавление колонок)
        if update_requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": update_requests}
            ).execute()
            logging.info("Структурные изменения (добавление столбца) успешно применены.")

        # Обновление значений
        if batch_update_values_data:
            body = {
                'valueInputOption': 'USER_ENTERED',
                'data': batch_update_values_data
            }
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()
        
        logging.info("✅ Batch-запрос на обновление ячеек успешно выполнен.")
        return True

    except Exception as e:
        logging.error(f"❌ Ошибка при выполнении batch-запроса: {e}")
        return False


def final_validation(service, spreadsheet_id, sheet_name, validated_articles, date_columns, db_data):
    """
    Проводит финальную валидацию: повторно читает данные из GS и сравнивает с данными из БД.
    """
    logging.info("Начало финальной валидации...")
    
    # 1. Повторное чтение данных из GS
    try:
        sheet_range = f"'{sheet_name}'!A:Z"
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_range).execute()
        sheet_data = result.get('values', [])
        if not sheet_data:
            logging.error("Финальная валидация не удалась: не удалось прочитать данные с листа.")
            return False
    except Exception as e:
        logging.error(f"Финальная валидация не удалась: ошибка при чтении данных с листа: {e}")
        return False

    # 2. Повторное сравнение
    mismatched_cells = []
    for nm_id, article_info in validated_articles.items():
        for attr_name, row_num in article_info['attributes'].items():
            if attr_name == "ДРР" or attr_name == "Журнал изменений":
                continue

            for date_obj, col_num in date_columns.items():
                try:
                    gs_value_str = sheet_data[row_num - 1][col_num - 1]
                except IndexError:
                    gs_value_str = ""
                
                db_record = db_data.get((nm_id, date_obj))
                db_value = db_record.get(attr_name, 0) if db_record else 0

                try:
                    if isinstance(gs_value_str, str) and '%' in gs_value_str:
                         gs_value = float(gs_value_str.replace('%', '').replace(',', '.')) / 100
                    elif isinstance(gs_value_str, str) and gs_value_str.strip():
                        gs_value = float(gs_value_str.replace(',', '.'))
                    elif gs_value_str == "":
                        gs_value = 0
                    else:
                        gs_value = float(gs_value_str)
                except (ValueError, TypeError):
                    gs_value = gs_value_str

                if not isinstance(gs_value, str) and not abs(float(gs_value) - float(db_value)) < 1e-9:
                    mismatched_cells.append(f"Артикул {nm_id}, Атрибут '{attr_name}', Дата {date_obj.strftime('%d.%m.%Y')}: GS='{gs_value}', DB='{db_value}'")

    if not mismatched_cells:
        logging.info("✅ Финальная валидация пройдена. Все данные в Google Sheets соответствуют данным из БД.")
        return True
    else:
        logging.error(f"❌ ОБНАРУЖЕНЫ РАСХОЖДЕНИЯ ПОСЛЕ ЗАПИСИ ({len(mismatched_cells)} ячеек):")
        for mismatch in mismatched_cells[:10]: # Логируем первые 10 расхождений
            logging.error(f"  - {mismatch}")
        return False


def main():
    """Главная функция для запуска процесса обновления отчета."""
    logging.info("Запуск скрипта обновления отчета РНП...")
    
    service = get_google_sheets_service()
    sheet_name = SHEET_NAMES.get(RNP_REPORT_ID)
    if not sheet_name:
        logging.error(f"Не найдено имя листа для ID таблицы {RNP_REPORT_ID}")
        return

    validated_articles, sheet_data = read_and_validate_structure(service, RNP_REPORT_ID, sheet_name)

    if not validated_articles:
        logging.info("Выполнение завершено из-за отсутствия валидных данных для обработки.")
        return
        
    logging.info("✅ Структура успешно прочитана и провалидирована.")

    header_row = sheet_data[0]
    logging.info(f"Содержимое первой строки (заголовка), как его видит скрипт: {header_row}")
    date_columns, add_column_req = get_and_validate_dates(service, RNP_REPORT_ID, sheet_name, header_row)

    if date_columns is None:
        return # Ошибка уже залогирована в функции

    logging.info("✅ Даты успешно просканированы и провалидированы.")

    db_data = get_data_from_db(validated_articles.keys(), date_columns.keys())
    if db_data is None:
        logging.error("Выполнение прервано из-за ошибки при получении данных из БД.")
        return

    logging.info("✅ Данные из БД успешно получены.")

    update_successful = compare_and_update(service, RNP_REPORT_ID, sheet_name, sheet_data, validated_articles, date_columns, db_data, add_column_req)

    if not update_successful:
        logging.error("Выполнение прервано из-за ошибки на этапе обновления данных.")
        return

    logging.info("✅ Данные в Google Sheets успешно обновлены.")

    final_validation(service, RNP_REPORT_ID, sheet_name, validated_articles, date_columns, db_data)


if __name__ == "__main__":
    main()
