"""
Трансформер данных для supplies_to_warehouses.
Фильтрует, извлекает и конвертирует данные для записи в БД.
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List
from decimal import Decimal
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def filter_accepted_supplies(incomes: list) -> list:
    """
    Фильтрует только записи со статусом "Принято".
    
    Args:
        incomes: Список всех записей поставок
        
    Returns:
        list: Отфильтрованные записи со статусом "Принято"
    """
    print("🔄 Фильтрация поставок по статусу 'Принято'...")
    
    accepted = []
    status_counts = {}
    
    for record in incomes:
        status = record.get("status", "Unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        
        if status == "Принято":
            accepted.append(record)
    
    print(f"📊 Статистика статусов: {status_counts}")
    print(f"✅ Отфильтровано записей 'Принято': {len(accepted)}")
    
    return accepted


def extract_delivery_expr(supply_details: dict) -> str | None:
    """
    Извлекает значение deliveryAndStorageExpr из JSON-RPC ответа.
    
    Args:
        supply_details: JSON-RPC ответ от supplyDetails API
        
    Returns:
        str | None: Значение коэффициента логистики или None
    """
    try:
        result = supply_details.get("result", {})
        supply = result.get("supply", {})
        delivery_storage = supply.get("deliveryAndStorage", {})
        expr_value = delivery_storage.get("deliveryAndStorageExpr")
        
        if expr_value is None:
            return None
        
        return str(expr_value)
        
    except Exception as e:
        print(f"❌ Ошибка извлечения deliveryAndStorageExpr: {e}")
        return None


def convert_delivery_expr_to_numeric(value: str | None) -> Decimal | None:
    """
    Конвертирует строковое значение deliveryAndStorageExpr в Decimal для БД.
    
    Args:
        value: Строковое значение коэффициента логистики
        
    Returns:
        Decimal | None: Конвертированное значение или None
    """
    if value is None or value == "":
        return None
    
    try:
        # Убираем возможные пробелы и конвертируем
        cleaned_value = str(value).strip()
        if not cleaned_value:
            return None
        
        # Конвертируем в Decimal для точности
        return Decimal(cleaned_value)
        
    except Exception as e:
        print(f"❌ Ошибка конвертации deliveryAndStorageExpr '{value}': {e}")
        return None


def parse_datetime_string(date_str: str) -> datetime:
    """
    Парсит строку даты в datetime объект.
    
    Args:
        date_str: Строка даты в формате ISO
        
    Returns:
        datetime: Объект datetime
    """
    try:
        # Пробуем разные форматы
        formats = [
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Если ничего не сработало, используем fromisoformat
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        
    except Exception as e:
        print(f"❌ Ошибка парсинга даты '{date_str}': {e}")
        # Возвращаем текущую дату как fallback
        return datetime.now(ZoneInfo("Europe/Moscow"))


def prepare_for_db(incomes: list) -> list[dict]:
    """
    Подготавливает данные для записи в БД.
    
    Args:
        incomes: Список записей поставок
        
    Returns:
        list[dict]: Подготовленные записи для БД
    """
    print("🔄 Подготовка данных для БД...")
    
    prepared_records = []
    
    for record in incomes:
        try:
            # Основные поля
            prepared_record = {
                "income_id": record["incomeId"],
                "nm_id": record["nmId"],
                "supplier_article": record["supplierArticle"],
                "barcode": record["barcode"],
                "tech_size": record.get("techSize"),
                "quantity": record["quantity"],
                "warehouse_name": record["warehouseName"],
                "number": record.get("number", ""),
            }
            
            # Парсим даты и конвертируем в строки для JSON
            date_obj = parse_datetime_string(record["date"])
            last_change_obj = parse_datetime_string(record["lastChangeDate"])
            
            prepared_record["date"] = date_obj.isoformat()
            prepared_record["last_change_date"] = last_change_obj.isoformat()
            
            # Добавляем delivery_and_storage_expr если есть
            if "delivery_and_storage_expr" in record:
                expr_str = record["delivery_and_storage_expr"]
                decimal_value = convert_delivery_expr_to_numeric(expr_str)
                # Конвертируем Decimal в float для JSON
                prepared_record["delivery_and_storage_expr"] = float(decimal_value) if decimal_value is not None else None
            
            prepared_records.append(prepared_record)
            
        except Exception as e:
            print(f"❌ Ошибка подготовки записи {record.get('incomeId', 'Unknown')}: {e}")
            continue
    
    print(f"✅ Подготовлено записей: {len(prepared_records)}")
    return prepared_records


def add_delivery_expr_to_records(records: list, delivery_data: dict[int, str | None]) -> list:
    """
    Добавляет delivery_and_storage_expr к записям.
    
    Args:
        records: Список записей поставок (уже трансформированных)
        delivery_data: Словарь {income_id: delivery_expr}
        
    Returns:
        list: Записи с добавленным delivery_and_storage_expr
    """
    print("🔄 Добавление delivery_and_storage_expr к записям...")
    
    updated_records = []
    added_count = 0
    
    for record in records:
        income_id = record["income_id"]  # уже трансформировано
        
        if income_id in delivery_data:
            record["delivery_and_storage_expr"] = delivery_data[income_id]
            added_count += 1
        
        updated_records.append(record)
    
    print(f"✅ Добавлено delivery_expr к {added_count} записям")
    return updated_records


def apply_fallback_delivery_expr(records: list, supabase) -> list:
    """
    Применяет fallback логику для записей без delivery_expr.
    Ищет предыдущие поставки того же товара на том же складе.
    
    Args:
        records: Список записей поставок
        supabase: Клиент Supabase
        
    Returns:
        list: Записи с примененным fallback delivery_expr
    """
    print("🔄 Применение fallback логики для записей без delivery_expr...")
    
    updated_records = []
    fallback_applied = 0
    
    for record in records:
        # Если delivery_expr уже есть, оставляем как есть
        if record.get("delivery_and_storage_expr") is not None:
            updated_records.append(record)
            continue
        
        # Ищем fallback delivery_expr
        warehouse_name = record["warehouse_name"]
        nm_id = record["nm_id"]
        current_date = record["date"]
        
        try:
            from excel_actions.supplies_to_warehouses_ea.supabase_writer import find_fallback_delivery_expr
            fallback_expr = find_fallback_delivery_expr(supabase, warehouse_name, nm_id, current_date)
            
            if fallback_expr is not None:
                record["delivery_and_storage_expr"] = fallback_expr
                fallback_applied += 1
                print(f"✅ Применен fallback delivery_expr: {fallback_expr} для {warehouse_name}, nm_id {nm_id}")
            else:
                print(f"⚠️  Fallback не найден для {warehouse_name}, nm_id {nm_id}")
                
        except Exception as e:
            print(f"❌ Ошибка применения fallback для {warehouse_name}, nm_id {nm_id}: {e}")
        
        updated_records.append(record)
    
    print(f"✅ Применено fallback delivery_expr к {fallback_applied} записям")
    return updated_records


def enrich_with_existing_delivery_expr(records: list, existing: dict) -> list:
    """
    Обогащает записи существующими delivery_expr из БД.
    
    Args:
        records: Список записей для обогащения
        existing: Словарь существующих записей из БД
        
    Returns:
        list: Обогащенные записи
    """
    print("🔄 Обогащение записей существующими delivery_expr из БД...")
    
    enriched_count = 0
    
    for record in records:
        income_id = record['income_id']
        nm_id = record['nm_id']
        key = (income_id, nm_id)
        
        # Если у записи уже есть delivery_expr, пропускаем
        if record.get("delivery_and_storage_expr") is not None:
            continue
            
        # Если запись есть в БД и у неё есть delivery_expr
        if key in existing and existing[key]['has_delivery_expr']:
            # Получаем delivery_expr из БД
            from excel_actions.supplies_to_warehouses_ea.supabase_writer import get_existing_delivery_expr
            existing_expr = get_existing_delivery_expr(existing, key)
            if existing_expr is not None:
                record["delivery_and_storage_expr"] = existing_expr
                enriched_count += 1
                print(f"✅ Обогащен delivery_expr: {existing_expr} для income_id {income_id}, nm_id {nm_id}")
    
    print(f"✅ Обогащено записей существующими delivery_expr: {enriched_count}")
    return records


if __name__ == "__main__":
    """Тестовый запуск трансформера"""
    import json
    
    # Загружаем тестовые данные
    test_file = PROJECT_ROOT / "incomes_response_20251026_114703.json"
    
    if not test_file.exists():
        print(f"❌ Тестовый файл не найден: {test_file}")
        sys.exit(1)
    
    print(f"📂 Загрузка тестового файла: {test_file}")
    with open(test_file, "r", encoding="utf-8") as f:
        test_data = json.load(f)
    
    try:
        # Тестируем фильтрацию
        accepted = filter_accepted_supplies(test_data)
        
        # Тестируем подготовку для БД
        prepared = prepare_for_db(accepted)
        
        print(f"\n✅ Трансформация прошла успешно!")
        print(f"📊 Исходных записей: {len(test_data)}")
        print(f"📊 Принятых записей: {len(accepted)}")
        print(f"📊 Подготовленных записей: {len(prepared)}")
        
        if prepared:
            sample = prepared[0]
            print(f"\n📋 Пример подготовленной записи:")
            for key, value in sample.items():
                print(f"  {key}: {value}")
                
    except Exception as e:
        print(f"\n❌ Ошибка трансформации:\n{e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
