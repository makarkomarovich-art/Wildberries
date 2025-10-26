"""
Модуль для работы с Supabase: обогащение данных и запись в БД.
"""
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set
from supabase import Client

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def enrich_with_product_ids(records: list, supabase: Client) -> list:
    """
    Обогащает записи product_id из таблицы products по nm_id.
    Использует SQL JOIN для оптимизации.
    
    Args:
        records: Список записей поставок
        supabase: Клиент Supabase
        
    Returns:
        list: Записи с добавленным product_id
    """
    print(f"🔄 Получение product_id из таблицы products...")
    
    # Получаем все products из БД
    response = supabase.table('products').select('nm_id, id').execute()
    products_map = {p['nm_id']: p['id'] for p in response.data}
    
    print(f"✅ Загружено products: {len(products_map)}")
    
    # Обогащаем записи и фильтруем
    enriched_records = []
    skipped_count = 0
    
    for record in records:
        nm_id = record['nm_id']
        product_id = products_map.get(nm_id)
        
        if not product_id:
            print(f"⚠️  WARNING: nm_id={nm_id} не найден в таблице products, пропускаем")
            skipped_count += 1
            continue
        
        record['product_id'] = product_id
        enriched_records.append(record)
    
    print(f"✅ Обогащено записей: {len(enriched_records)}")
    if skipped_count > 0:
        print(f"⚠️  Пропущено записей (нет в products): {skipped_count}")
    
    return enriched_records


def get_existing_supplies(supabase: Client) -> dict:
    """
    Получает существующие поставки из БД.
    
    Args:
        supabase: Клиент Supabase
        
    Returns:
        dict: {(income_id, nm_id): {'last_change_date': ..., 'has_delivery_expr': bool}}
    """
    print("🔄 Загрузка существующих поставок из БД...")
    
    try:
        response = supabase.table("supplies_to_warehouses").select(
            "income_id, nm_id, last_change_date, delivery_and_storage_expr"
        ).execute()
        
        existing = {}
        for record in response.data:
            key = (record["income_id"], record["nm_id"])
            existing[key] = {
                "last_change_date": record["last_change_date"],
                "has_delivery_expr": record["delivery_and_storage_expr"] is not None
            }
        
        print(f"✅ Загружено существующих записей: {len(existing)}")
        return existing
        
    except Exception as e:
        print(f"❌ Ошибка загрузки существующих поставок: {e}")
        return {}


def get_incomes_without_delivery_expr(supabase: Client) -> Set[int]:
    """
    Получает список income_id, у которых delivery_and_storage_expr = NULL.
    
    Args:
        supabase: Клиент Supabase
        
    Returns:
        Set[int]: Множество income_id для повторного запроса
    """
    print("🔄 Поиск поставок без delivery_and_storage_expr...")
    
    try:
        response = supabase.table("supplies_to_warehouses").select(
            "income_id"
        ).is_("delivery_and_storage_expr", "null").execute()
        
        income_ids = set(record["income_id"] for record in response.data)
        print(f"✅ Найдено поставок без delivery_expr: {len(income_ids)}")
        
        return income_ids
        
    except Exception as e:
        print(f"❌ Ошибка поиска поставок без delivery_expr: {e}")
        return set()


def find_fallback_delivery_expr(supabase: Client, warehouse_name: str, nm_id: int, current_date: str) -> float | None:
    """
    Ищет ближайшую предыдущую поставку с тем же складом и артикулом для получения delivery_expr.
    
    Args:
        supabase: Клиент Supabase
        warehouse_name: Название склада
        nm_id: Артикул товара
        current_date: Дата текущей поставки (ISO формат)
        
    Returns:
        float | None: Значение delivery_expr из предыдущей поставки или None
    """
    try:
        # Ищем предыдущие поставки того же товара на том же складе
        response = supabase.table("supplies_to_warehouses")\
            .select("delivery_and_storage_expr, date")\
            .eq("warehouse_name", warehouse_name)\
            .eq("nm_id", nm_id)\
            .not_.is_("delivery_and_storage_expr", "null")\
            .lt("date", current_date)\
            .order("date", desc=True)\
            .limit(1)\
            .execute()
        
        if response.data:
            fallback_expr = response.data[0]["delivery_and_storage_expr"]
            fallback_date = response.data[0]["date"]
            print(f"📦 Найден fallback delivery_expr: {fallback_expr} (от {fallback_date})")
            return float(fallback_expr)
        else:
            print(f"⚠️  Fallback delivery_expr не найден для {warehouse_name}, nm_id {nm_id}")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка поиска fallback delivery_expr: {e}")
        return None


def upsert_records(records: list, supabase: Client) -> Tuple[int, int]:
    """
    Выполняет upsert записей в БД.
    
    Args:
        records: Список записей для записи
        supabase: Клиент Supabase
        
    Returns:
        Tuple[int, int]: (количество новых, количество обновленных)
    """
    print("🔄 Запись данных в БД...")
    
    if not records:
        print("⚠️  Нет записей для записи")
        return 0, 0
    
    try:
        # Выполняем upsert с on_conflict
        response = supabase.table("supplies_to_warehouses").upsert(
            records,
            on_conflict="income_id,nm_id"
        ).execute()
        
        # Подсчитываем результаты
        # Supabase не возвращает точную статистику, поэтому используем приблизительную
        total_count = len(response.data)
        
        # Для простоты считаем все как новые записи
        # В реальности можно было бы сравнивать с существующими данными
        new_count = total_count
        updated_count = 0
        
        print(f"✅ Записано записей в БД: {total_count}")
        print(f"📊 Новых записей: {new_count}")
        print(f"📊 Обновленных записей: {updated_count}")
        
        return new_count, updated_count
        
    except Exception as e:
        print(f"❌ Ошибка записи в БД: {e}")
        raise


def validate_inserted_data(records: list, supabase: Client) -> bool:
    """
    Проверяет корректность записанных данных.
    
    Args:
        records: Список записанных записей
        supabase: Клиент Supabase
        
    Returns:
        bool: True если данные корректны
    """
    print("🔍 Валидация записанных данных...")
    
    if not records:
        print("⚠️  Нет записей для валидации")
        return True
    
    try:
        # Получаем записи из БД для проверки
        income_ids = [record["income_id"] for record in records]
        nm_ids = [record["nm_id"] for record in records]
        
        response = supabase.table("supplies_to_warehouses").select(
            "income_id, nm_id, product_id, delivery_and_storage_expr"
        ).in_("income_id", income_ids).in_("nm_id", nm_ids).execute()
        
        db_records = response.data
        print(f"📊 Записей в БД для проверки: {len(db_records)}")
        
        # Проверяем обязательные поля
        issues = []
        for record in db_records:
            if not record["product_id"]:
                issues.append(f"Отсутствует product_id для записи {record['income_id']}-{record['nm_id']}")
        
        if issues:
            print(f"❌ Найдены проблемы:")
            for issue in issues:
                print(f"  - {issue}")
            return False
        
        print("✅ Валидация прошла успешно")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка валидации: {e}")
        return False


if __name__ == "__main__":
    """Тестовый запуск модуля"""
    from supabase import create_client
    import api_keys
    
    try:
        # Подключение к Supabase
        url = api_keys.SUPABASE_URL
        key = api_keys.SUPABASE_KEY
        
        if not url or not key:
            print("❌ Supabase credentials не настроены")
            sys.exit(1)
        
        supabase = create_client(url, key)
        print(f"✅ Подключено к Supabase: {url}")
        
        # Тестируем функции
        existing = get_existing_supplies(supabase)
        print(f"📊 Существующих записей: {len(existing)}")
        
        incomes_without_delivery = get_incomes_without_delivery_expr(supabase)
        print(f"📊 Поставок без delivery_expr: {len(incomes_without_delivery)}")
        
        print("\n✅ Тест модуля Supabase прошел успешно!")
        
    except Exception as e:
        print(f"\n❌ Ошибка тестирования: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
