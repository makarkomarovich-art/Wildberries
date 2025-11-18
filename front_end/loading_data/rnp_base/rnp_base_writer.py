"""
Запись данных в Google Sheets и создание backup.
"""

import logging
from typing import List, Tuple, Optional, Any
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


def get_google_sheets_service(credentials_info: dict):
    """
    Инициализирует Google Sheets API service.
    
    Args:
        credentials_info: Словарь с учетными данными из api_keys.GOOGLE_CREDENTIALS_INFO
    
    Returns:
        Google Sheets service object
    """
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    service = build('sheets', 'v4', credentials=credentials)
    return service


def check_or_create_sheet(
    service,
    spreadsheet_id: str,
    sheet_name: str
) -> Tuple[bool, int]:
    """
    Проверяет наличие листа. Если его нет - создаёт.
    
    Args:
        service: Google Sheets service
        spreadsheet_id: ID таблицы
        sheet_name: Название листа
    
    Returns:
        (exists, sheet_id)
    """
    logging.info(f"🔍 Проверка листа '{sheet_name}'...")
    
    try:
        # Получаем метаданные таблицы
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        sheets = spreadsheet.get('sheets', [])
        
        # Ищем лист с нужным названием
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                logging.info(f"✅ Лист '{sheet_name}' существует (id={sheet['properties']['sheetId']})")
                return True, sheet['properties']['sheetId']
        
        # Лист не найден - создаём
        logging.info(f"📝 Лист '{sheet_name}' не существует, создаю...")
        
        request = {
            'addSheet': {
                'properties': {
                    'title': sheet_name,
                    'gridProperties': {
                        'rowCount': 10000,
                        'columnCount': 26,
                    }
                }
            }
        }
        
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': [request]}
        ).execute()
        
        new_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
        logging.info(f"✅ Лист '{sheet_name}' создан (id={new_sheet_id})")
        return False, new_sheet_id
    
    except Exception as e:
        logging.error(f"❌ Ошибка при проверке/создании листа: {e}")
        raise


def clear_sheet_data(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    start_row: int = 2
) -> bool:
    """
    Очищает данные листа (строки от start_row).
    Хедеры (строка 1) НЕ трогаем.
    
    Args:
        start_row: С какой строки начинать очистку (1-based, по умолчанию с 2-й)
    
    Returns:
        True если успешно
    """
    logging.info(f"🗑️  Очистка данных листа '{sheet_name}' (строки с {start_row})...")
    
    try:
        # Очищаем большой диапазон (от start_row до 10000)
        clear_range = f"'{sheet_name}'!A{start_row}:Z10000"
        
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=clear_range
        ).execute()
        
        logging.info(f"✅ Очистка завершена")
        return True
    
    except Exception as e:
        logging.error(f"❌ Ошибка при очистке листа: {e}")
        return False


def write_headers(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    headers: List[str]
) -> bool:
    """
    Пишет хедеры в строку 1 листа.
    
    Args:
        headers: Список названий хедеров
    
    Returns:
        True если успешно
    """
    logging.info(f"📝 Запись хедеров в лист '{sheet_name}'...")
    
    try:
        update_range = f"'{sheet_name}'!A1:Z1"
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption="RAW",
            body={'values': [headers]}
        ).execute()
        
        logging.info(f"✅ Хедеры записаны ({len(headers)} колонок)")
        return True
    
    except Exception as e:
        logging.error(f"❌ Ошибка при записи хедеров: {e}")
        return False


def write_data_rows(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    data_rows: List[List[Any]],
    start_row: int = 2
) -> Tuple[bool, int]:
    """
    Пишет данные в лист.
    
    Args:
        data_rows: Список списков (каждый список = одна строка)
        start_row: С какой строки начинать запись (1-based)
    
    Returns:
        (success, rows_written)
    """
    logging.info(f"💾 Запись {len(data_rows)} строк в лист '{sheet_name}'...")
    
    if not data_rows:
        logging.warning("⚠️  Нет данных для записи")
        return True, 0
    
    try:
        # Вычисляем диапазон
        num_cols = len(data_rows[0])
        col_letter = chr(64 + num_cols) if num_cols <= 26 else 'Z'
        end_row = start_row + len(data_rows) - 1
        
        update_range = f"'{sheet_name}'!A{start_row}:{col_letter}{end_row}"
        
        response = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption="USER_ENTERED",
            body={'values': data_rows}
        ).execute()
        
        updated_rows = response.get('updates', {}).get('updatedRows', len(data_rows))
        logging.info(f"✅ Запись завершена ({updated_rows} строк, {response.get('updates', {}).get('updatedColumns', num_cols)} колонок)")
        
        return True, updated_rows
    
    except Exception as e:
        logging.error(f"❌ Ошибка при записи данных: {e}")
        return False, 0


def format_date_column(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    num_rows: int
) -> bool:
    """
    Применяет форматирование даты (DD.MM.YYYY) к колонке A.
    
    Args:
        num_rows: Количество строк данных (не считая хедера)
    
    Returns:
        True если успешно
    """
    logging.info(f"📅 Применение формата даты DD.MM.YYYY к колонке A...")
    
    try:
        # Получаем sheet_id
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        
        sheet_id = None
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                sheet_id = sheet['properties']['sheetId']
                break
        
        if sheet_id is None:
            logging.warning(f"⚠️  Лист '{sheet_name}' не найден для форматирования")
            return False
        
        # Формируем запрос на форматирование
        requests = [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 1,  # Начинаем со строки 2 (index 1)
                        'endRowIndex': num_rows + 1,  # До конца данных
                        'startColumnIndex': 0,  # Колонка A
                        'endColumnIndex': 1,  # Только колонка A
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'DATE',
                                'pattern': 'DD.MM.YYYY'
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat'
                }
            }
        ]
        
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': requests}
        ).execute()
        
        logging.info(f"✅ Формат даты применен к {num_rows} строкам")
        return True
    
    except Exception as e:
        logging.error(f"❌ Ошибка при форматировании даты: {e}")
        return False


def create_backup(
    service,
    spreadsheet_id: str,
    main_sheet_name: str,
    backup_sheet_name: str,
    backup_data: List[List[Any]],
    headers: List[str]
) -> bool:
    """
    Создает или обновляет backup лист.
    ОЧИЩАЕТ старый backup перед записью новых данных!
    
    Args:
        backup_data: Текущие данные из основного листа (для backup)
        headers: Хедеры
    
    Returns:
        True если успешно
    """
    logging.info(f"🔄 Создание backup листа '{backup_sheet_name}'...")
    
    try:
        # 1. Проверяем/создаём лист backup
        exists, backup_sheet_id = check_or_create_sheet(service, spreadsheet_id, backup_sheet_name)
        
        # 2. ОЧИЩАЕМ старый backup (все строки от 2-й)
        logging.info(f"🗑️  Очистка старого backup перед новыми данными...")
        clear_sheet_data(service, spreadsheet_id, backup_sheet_name, start_row=2)
        
        # 3. Пишем хедеры в backup
        write_headers(service, spreadsheet_id, backup_sheet_name, headers)
        
        # 4. Если есть данные - пишем их
        if backup_data and len(backup_data) > 1:  # >1 потому что первая строка - хедеры
            # Пропускаем первую строку (хедеры)
            data_only = backup_data[1:] if len(backup_data) > 1 else []
            
            write_data_rows(service, spreadsheet_id, backup_sheet_name, data_only, start_row=2)
            logging.info(f"✅ Backup создан ({len(data_only)} строк данных)")
        else:
            logging.info(f"✅ Backup лист создан (пустой)")
        
        return True
    
    except Exception as e:
        logging.error(f"❌ Ошибка при создании backup: {e}")
        return False


def write_update_timestamp(
    service,
    spreadsheet_id: str,
    sheet_name: str
) -> bool:
    """
    Записывает время последнего обновления в ячейку R1 листа.
    
    Args:
        service: Google Sheets service
        spreadsheet_id: ID таблицы
        sheet_name: Название листа
    
    Returns:
        True если успешно
    """
    from datetime import datetime
    
    logging.info(f"⏰ Запись времени обновления в ячейку R1...")
    
    try:
        timestamp_str = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        
        # Записываем время обновления в ячейку R1
        update_range = f"'{sheet_name}'!R1"
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption="USER_ENTERED",
            body={'values': [[f"Последнее обновление: {timestamp_str}"]]}
        ).execute()
        
        logging.info(f"✅ Время обновления записано: {timestamp_str}")
        return True
    
    except Exception as e:
        logging.error(f"❌ Ошибка при записи времени обновления: {e}")
        return False

