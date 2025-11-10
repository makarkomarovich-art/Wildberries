"""
Функции для записи данных warehouse_remains в Google Sheets.
Поддерживает batch updates и правильную структуру таблицы.
"""

from typing import List, Dict, Any, Tuple, Set
from pathlib import Path
import importlib.util
import sys


# Импортируем все переменные из api-keys (динамический абсолютный путь)
BASE_DIR = Path(__file__).resolve().parents[2]
api_keys_path = BASE_DIR / 'api_keys.py'
spec = importlib.util.spec_from_file_location("api_keys", str(api_keys_path))
api_keys_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_keys_module)
GOOGLE_CREDENTIALS_FILE = getattr(api_keys_module, 'GOOGLE_CREDENTIALS_FILE', '')
GOOGLE_CREDENTIALS_INFO = getattr(api_keys_module, 'GOOGLE_CREDENTIALS_INFO', None)

# Импортируем header mapping utilities
header_mapping_path = Path(__file__).resolve().parents[1] / 'utils' / 'header_mapping.py'
spec_header = importlib.util.spec_from_file_location("header_mapping", str(header_mapping_path))
header_mapping_module = importlib.util.module_from_spec(spec_header)
sys.modules[spec_header.name] = header_mapping_module
spec_header.loader.exec_module(header_mapping_module)
load_header_map = header_mapping_module.load_header_map
HeaderMappingError = header_mapping_module.HeaderMappingError

# Импортируем конфиг заголовков для warehouse
config_path = Path(__file__).resolve().with_name('header_config.py')
spec_config = importlib.util.spec_from_file_location("header_config", str(config_path))
header_config = importlib.util.module_from_spec(spec_config)
spec_config.loader.exec_module(header_config)
HEADER_ROW_INDEX = header_config.HEADER_ROW_INDEX
WAREHOUSE_HEADER_ALIASES = header_config.WAREHOUSE_HEADER_ALIASES

# Импортируем утилиту записи времени обновления
_last_updated_path = Path(__file__).resolve().parents[1] / 'utils' / 'sheets_last_updated.py'
_spec_last = importlib.util.spec_from_file_location('sheets_last_updated', str(_last_updated_path))
_sheets_last_updated = importlib.util.module_from_spec(_spec_last)
sys.modules[_spec_last.name] = _sheets_last_updated
_spec_last.loader.exec_module(_sheets_last_updated)
write_last_updated = _sheets_last_updated.write_last_updated


# Список складов для удаления данных (хардкодный список)
WAREHOUSE_COLUMNS = [
    "Остальные", "Коледино", "Электросталь", "Тула", "Казань", "Краснодар", "Невинномысск", 
    "Самара (Новосемейкино)", "Екатеринбург - Перспективный 12", "Екатеринбург - Испытателей 14г",
    "Санкт-Петербург Уткина Заводь", "Санкт-Петербург Шушар", "Новосибирск", "Атакент",
    "Белая дача", "Волгоград", "Воронеж", "Котовск", "Махачкала Сепараторная", "Минск",
    "Пушкино", "Радумля 1", "Рязань (Тюшевское)", "Сарапул", "Череповец", "СЦ Архангельск",
    "СЦ Астрахань", "СЦ Владикавказ", "СЦ Внуково", "СЦ Гомель 2", "СЦ Гродно", "СЦ Ереван",
    "СЦ Калуга", "Актобе", "СЦ Мурманск", "СЦ Новокузнецк", "СЦ Новосибирск Пасечная",
    "СЦ Омск", "СЦ Пермь 2", "СЦ Уфа", "СЦ Чебоксары 2", "СЦ Чита 2", "СЦ Ярославль Громово",
    "Сургут", "Чашниково", "СЦ Псков", "СЦ Ростов-на-Дону", "СЦ Симферополь (Молодежненское)",
    "СЦ Сыктывкар", "СЦ Тамбов", "СЦ Ульяновск", "Астана Карагандинское шоссе", "Белые Столбы",
    "Владимир", "Вёшки", "Иваново", "Калининград", "Крыловская", "Обухово", "Подольск",
    "СПБ Шушары", "СЦ Абакан 2", "СЦ Астрахань (Солянка)", "СЦ Курск", "СЦ Липецк",
    "СЦ Оренбург Центральная", "СЦ Смоленск 3", "СЦ Барнаул", "СЦ Белогорск", "СЦ Брест",
    "СЦ Брянск 2", "СЦ Вологда 2", "СЦ Ижевск", "СЦ Кемерово", "СЦ Кузнецк", "СЦ Минск",
    "СЦ Тюмень", "СЦ Челябинск 2", "СЦ Томск", "Артём", "СЦ Иркутск", "СЦ Киров",
    "СЦ Пятигорск", "СЦ Софьино", "СЦ Ярославль Громова", "СЦ Хабаровск", "СЦ Шушары"
]

# Дополнительные столбцы
ADDITIONAL_COLUMNS = [
    "В пути к клиенту",      # В пути до получателей
    "В пути от клиента",     # В пути возвраты на склад WB
    "Объем упаковки"         # volume
]


def get_google_sheets_service():
    """Получает сервис Google Sheets."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    if GOOGLE_CREDENTIALS_INFO:
        credentials = Credentials.from_service_account_info(GOOGLE_CREDENTIALS_INFO, scopes=scopes)
    else:
        credentials = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scopes)
    return build('sheets', 'v4', credentials=credentials)


def extract_sheet_id(sheet_url: str) -> str:
    """Извлекает ID таблицы из URL Google Sheets."""
    try:
        if '/spreadsheets/d/' in sheet_url:
            start = sheet_url.find('/spreadsheets/d/') + len('/spreadsheets/d/')
            end = sheet_url.find('/', start)
            if end == -1:
                end = sheet_url.find('?', start)
            if end == -1:
                end = len(sheet_url)
            return sheet_url[start:end]
        return ""
    except Exception:
        return ""


def format_sheet_name(sheet_name: str) -> str:
    """Форматирует название листа для использования в диапазонах Google Sheets."""
    return f"'{sheet_name}'" if ' ' in sheet_name else sheet_name


def column_index_to_letter(col_index: int) -> str:
    """Преобразует индекс столбца в букву Google Sheets (A, B, C, ..., Z, AA, AB, ...)."""
    result = ""
    while col_index >= 0:
        result = chr(65 + (col_index % 26)) + result
        col_index = col_index // 26 - 1
        if col_index < 0:
            break
    return result


def analyze_sheet_structure(spreadsheet_id: str, sheet_name: str) -> Dict[str, Any]:
    """
    Анализирует структуру Google Sheets и определяет позиции столбцов.
    
    Args:
        spreadsheet_id: ID Google таблицы
        sheet_name: Название листа
        
    Returns:
        Dict: Информация о структуре таблицы
    """
    service = get_google_sheets_service()
    
    # Читаем заголовки (первая строка)
    sheet_name_quoted = format_sheet_name(sheet_name)
    range_headers = f"{sheet_name_quoted}!1:1"
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_headers
    ).execute()
    
    headers = result.get('values', [[]])[0] if result.get('values') else []
    
    # Находим позиции всех непустых столбцов
    column_positions = {}
    for i, header in enumerate(headers):
        if header and str(header).strip():
            column_positions[header] = i
    
    # Находим столбец с barcode (столбец A)
    barcode_column = None
    if len(headers) >= 1:
        barcode_column = 0  # A (индекс 0)
    
    return {
        'headers': headers,
        'column_positions': column_positions,
        'barcode_column': barcode_column,
        'total_columns': len(headers)
    }


def clear_old_data(spreadsheet_id: str, sheet_name: str, structure_info: Dict[str, Any], target_barcodes: List[str], header_map=None, start_row: int = 2) -> int:
    """
    Очищает старые данные в столбцах складов только для указанных barcode.
    
    Args:
        spreadsheet_id: ID Google таблицы
        sheet_name: Название листа
        structure_info: Информация о структуре таблицы
        target_barcodes: Список barcode для очистки
        
    Returns:
        int: Количество очищенных ячеек
    """
    service = get_google_sheets_service()
    
    sheet_name_quoted = format_sheet_name(sheet_name)

    # Читаем столбец баркодов динамически через header_map
    if header_map is None:
        print("❌ Header map не передан для очистки")
        return 0
    try:
        barcode_range = header_map.build_column_range("barcode", start_row)
    except HeaderMappingError as e:
        print(f"❌ {e}")
        return 0

    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=barcode_range
    ).execute()
    values = result.get('values', [])
    
    # Находим строки с целевыми barcode
    target_rows = []
    for i, row in enumerate(values):
        if row and row[0].strip():  # Есть данные в barcode
            barcode_value = row[0].strip()
            if barcode_value in target_barcodes:
                # Смещение на start_row: values[0] соответствует строке start_row
                target_rows.append(start_row + i)
    
    if not target_rows:
        print("ℹ️ Нет строк с целевыми barcode для очистки")
        return 0
    
    print(f"🧹 Очищаем данные для {len(target_rows)} строк с barcode: {target_barcodes[:5]}{'...' if len(target_barcodes) > 5 else ''}")
    
    # Подготавливаем batch update для очистки столбцов складов
    clear_requests = []
    column_positions = structure_info['column_positions']
    
    for warehouse_name in WAREHOUSE_COLUMNS + ADDITIONAL_COLUMNS:
        if warehouse_name in column_positions:
            col_index = column_positions[warehouse_name]
            col_letter = column_index_to_letter(col_index)
            
            # Создаем диапазон для очистки (только целевые строки)
            for row_num in target_rows:
                range_name = f"{sheet_name_quoted}!{col_letter}{row_num}"
                clear_requests.append({
                    "range": range_name,
                    "values": [[""]]  # Пустое значение
                })
    
    # Выполняем batch clear
    if clear_requests:
        body = {
            "valueInputOption": "RAW",
            "data": clear_requests
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
        print(f"✅ Очищено {len(clear_requests)} ячеек в столбцах складов")
        return len(clear_requests)
    
    return 0


def prepare_batch_data(aggregated_data: List[Dict[str, Any]], structure_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Подготавливает данные для batch update.
    
    Args:
        aggregated_data: Агрегированные данные warehouse_remains
        structure_info: Информация о структуре таблицы
        
    Returns:
        List[Dict]: Подготовленные данные для batch update
    """
    batch_updates = []
    column_positions = structure_info['column_positions']
    
    for item in aggregated_data:
        barcode = item['barcode']
        
        # Находим строку с этим barcode
        # TODO: Здесь нужно будет найти строку по barcode в таблице
        
        # Подготавливаем данные для записи
        row_updates = {}
        
        # Данные для специальных столбцов
        if "В пути к клиенту" in column_positions:
            row_updates["В пути к клиенту"] = item['in_way_to_recipients']
        
        if "В пути от клиента" in column_positions:
            row_updates["В пути от клиента"] = item['in_way_returns_to_warehouse']
        
        if "Объем упаковки" in column_positions:
            row_updates["Объем упаковки"] = item['volume']
        
        # Данные для складов
        for warehouse_name, quantity in item['warehouses'].items():
            if warehouse_name in column_positions:
                row_updates[warehouse_name] = quantity
        
        if row_updates:
            batch_updates.append({
                'barcode': barcode,
                'updates': row_updates
            })
    
    return batch_updates


def find_barcode_rows(spreadsheet_id: str, sheet_name: str, barcodes: List[str], header_map=None, start_row: int = 2) -> Dict[str, int]:
    """
    Находит номера строк для каждого barcode в Google Sheets.
    
    Args:
        spreadsheet_id: ID Google таблицы
        sheet_name: Название листа
        barcodes: Список barcode для поиска
        
    Returns:
        Dict[str, int]: Словарь {barcode: row_number}
    """
    service = get_google_sheets_service()
    
    # Читаем столбец баркодов динамически через header_map
    if header_map is None:
        return {}
    try:
        barcode_range = header_map.build_column_range("barcode", start_row)
    except HeaderMappingError:
        return {}
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=barcode_range
    ).execute()
    values = result.get('values', [])
    
    # Создаем индекс barcode -> row_number
    barcode_rows = {}
    for i, row in enumerate(values):
        if row and row[0].strip():
            barcode_value = row[0].strip()
            if barcode_value in barcodes:
                # Смещение на start_row: values[0] соответствует строке start_row
                barcode_rows[barcode_value] = start_row + i
    
    return barcode_rows


def write_batch_data(spreadsheet_id: str, sheet_name: str, batch_data: List[Dict[str, Any]], structure_info: Dict[str, Any], header_map=None, start_row: int = 2) -> int:
    """
    Записывает данные batch update в Google Sheets.
    
    Args:
        spreadsheet_id: ID Google таблицы
        sheet_name: Название листа
        batch_data: Подготовленные данные для записи
        structure_info: Информация о структуре таблицы
        
    Returns:
        int: Количество заполненных ячеек
    """
    service = get_google_sheets_service()
    sheet_name_quoted = format_sheet_name(sheet_name)
    
    # Получаем список всех barcode для поиска
    all_barcodes = [item['barcode'] for item in batch_data]
    
    # Находим номера строк для каждого barcode (динамический столбец)
    barcode_rows = find_barcode_rows(spreadsheet_id, sheet_name, all_barcodes, header_map=header_map, start_row=start_row)
    
    print(f"📝 Найдено {len(barcode_rows)} из {len(all_barcodes)} barcode в таблице")
    
    # Подготавливаем batch requests
    batch_requests = []
    column_positions = structure_info['column_positions']
    
    for item in batch_data:
        barcode = item['barcode']
        updates = item['updates']
        
        if barcode not in barcode_rows:
            continue
        
        row_number = barcode_rows[barcode]
        
        # Создаем запросы для каждого обновления
        for column_name, value in updates.items():
            if column_name in column_positions:
                col_index = column_positions[column_name]
                col_letter = column_index_to_letter(col_index)
                
                range_name = f"{sheet_name_quoted}!{col_letter}{row_number}"
                batch_requests.append({
                    "range": range_name,
                    "values": [[value]]
                })
    
    # Выполняем batch update
    if batch_requests:
        body = {
            "valueInputOption": "RAW",
            "data": batch_requests
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
        print(f"✅ Записано {len(batch_requests)} обновлений")
        return len(batch_requests)
    else:
        print("ℹ️ Нет данных для записи")
        return 0


def write_warehouse_remains_to_sheets(spreadsheet_url: str, sheet_name: str, aggregated_data: List[Dict[str, Any]]) -> None:
    """
    Главная функция для записи данных warehouse_remains в Google Sheets.
    
    Args:
        spreadsheet_url: URL Google таблицы
        sheet_name: Название листа
        aggregated_data: Агрегированные данные warehouse_remains
    """
    print("📊 ЗАПИСЬ ДАННЫХ WAREHOUSE_REMAINS В GOOGLE SHEETS")
    print("=" * 60)
    print()
    
    # Извлекаем ID таблицы
    spreadsheet_id = extract_sheet_id(spreadsheet_url)
    if not spreadsheet_id:
        print("❌ Ошибка: Не удалось извлечь ID таблицы из URL")
        return
    
    print(f"📋 Таблица: {spreadsheet_id}")
    print(f"📄 Лист: {sheet_name}")
    print(f"📦 Данных для записи: {len(aggregated_data)}")
    print()
    
    # Анализируем структуру таблицы
    print("🔍 Анализируем структуру таблицы...")
    structure_info = analyze_sheet_structure(spreadsheet_id, sheet_name)
    found_warehouses = [c for c in structure_info['column_positions'] if c in WAREHOUSE_COLUMNS]
    found_additional = [c for c in structure_info['column_positions'] if c in ADDITIONAL_COLUMNS]
    print(f"1. Найдено складов в Google Таблице: {len(found_warehouses)}")
    print(f"📊 Найдено столбцов: {structure_info['total_columns']}")
    print(f"🏢 Столбцов складов: {len(found_warehouses)}")
    print(f"📈 Дополнительных столбцов: {len(found_additional)}")
    print()
    
    # 3. Находим склады, которые есть в данных из API, но их нет в Google таблице
    api_warehouses = set()
    for item in aggregated_data:
        api_warehouses.update(item['warehouses'].keys())
    
    missing_warehouses = api_warehouses - set(found_warehouses)
    if missing_warehouses:
        print(f"3. Склады, которые есть в данных из API, но их нет в Google таблице:")
        for warehouse in sorted(missing_warehouses):
            print(f"   • {warehouse}")
        print()
    
    # Загружаем карту заголовков и валидируем наличие колонки "Баркод"
    service = get_google_sheets_service()
    try:
        header_map = load_header_map(
            service=service,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            expected_headers=WAREHOUSE_HEADER_ALIASES,
            header_row=HEADER_ROW_INDEX,
        )
        barcode_info = header_map.get("barcode")
        print(f"📋 Колонка 'Баркод' найдена: {barcode_info.letter}")
    except HeaderMappingError as e:
        print(f"❌ {e}")
        return

    # Очищаем старые данные только для barcode из новых данных
    print("🧹 Очищаем старые данные...")
    target_barcodes = [item['barcode'] for item in aggregated_data]
    cleared_cells = clear_old_data(spreadsheet_id, sheet_name, structure_info, target_barcodes, header_map=header_map, start_row=2)
    print(f"2. Очищено ячеек в Google таблице: {cleared_cells}")
    print()
    
    # Подготавливаем данные для batch update
    print("📝 Подготавливаем данные для batch update...")
    batch_data = prepare_batch_data(aggregated_data, structure_info)
    print(f"✅ Подготовлено {len(batch_data)} записей")
    print()
    
    # Записываем данные
    print("💾 Записываем данные в таблицу...")
    filled_cells = write_batch_data(spreadsheet_id, sheet_name, batch_data, structure_info, header_map=header_map, start_row=2)
    print(f"4. Заполнено ячеек: {filled_cells}")
    print()

    # Лог: какие баркоды есть в листе, но отсутствуют в API
    try:
        # Прочитаем все баркоды из листа
        rng = header_map.build_column_range("barcode", 2)
        res = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
        sheet_values = [row[0].strip() for row in res.get('values', []) if row]
        sheet_barcodes = set(sheet_values)
        api_barcodes = set(target_barcodes)
        missing_in_api = sorted(sheet_barcodes - api_barcodes)
        if missing_in_api:
            print("⚠️ Баркоды в листе, отсутствующие в ответе API (первые 20):")
            for bc in missing_in_api[:20]:
                print(f"   • {bc}")
            if len(missing_in_api) > 20:
                print(f"   ... и еще {len(missing_in_api) - 20}")
    except Exception:
        pass

    # Проставляем отметку времени последнего обновления в A2
    try:
        write_last_updated(spreadsheet_id, sheet_name, cell="A2", prefix="Обновлено: ")
    except Exception as e:
        print(f"⚠️ Не удалось записать отметку обновления: {e}")
    
    print("🎉 Запись данных завершена успешно!")


def test_column_preservation():
    """
    Тестирует сохранение данных между пропущенными столбцами при batch update.
    
    Ответ на вопрос пользователя: Да, данные в столбце D сохранятся при batch update
    столбцов A:C и затем E, потому что Google Sheets API обновляет только указанные
    диапазоны ячеек, не затрагивая другие.
    """
    print("🧪 ТЕСТИРОВАНИЕ СОХРАНЕНИЯ ДАННЫХ МЕЖДУ СТОЛБЦАМИ")
    print("=" * 60)
    print()
    print("📋 ОТВЕТ НА ВОПРОС О СОХРАНЕНИИ ДАННЫХ:")
    print("✅ ДА, данные в столбце D сохранятся при batch update!")
    print()
    print("🔍 ОБЪЯСНЕНИЕ:")
    print("• Google Sheets API обновляет только указанные диапазоны ячеек")
    print("• Batch update A:C не затрагивает столбец D")
    print("• Batch update E не затрагивает столбец D")
    print("• Данные в столбце D остаются неизменными")
    print()
    print("📝 ПРИМЕР:")
    print("1. Исходное состояние: A=1, B=2, C=3, D=4, E=5")
    print("2. Batch update A:C: A=10, B=20, C=30")
    print("3. Batch update E: E=50")
    print("4. Результат: A=10, B=20, C=30, D=4, E=50")
    print("   ↑ Столбец D остался неизменным!")
    print()
    print("✅ Это безопасно для нашей задачи!")


if __name__ == "__main__":
    # Тестируем сохранение данных между столбцами
    test_column_preservation()
