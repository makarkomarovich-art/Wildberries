"""
Функции для записи данных CR Daily Stats в Supabase.

Отвечает за:
- Обогащение записей product_id из таблицы products
- Upsert записей в таблицу cr_daily_stats
"""

from __future__ import annotations
from typing import List
from supabase import Client


def enrich_with_product_ids(
    records: list[dict],
    supabase: Client
) -> list[dict]:
    """
    Обогащает записи product_id из таблицы products.
    Фильтрует записи, для которых не найден product_id.
    
    Args:
        records: Список записей для обогащения
        supabase: Клиент Supabase
    
    Returns:
        Список записей с добавленным product_id (отфильтрованный)
    """
    if not records:
        return []
    
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


def upsert_records(
    records: list[dict],
    supabase: Client,
    label: str = ""
) -> int:
    """
    Выполняет upsert записей в таблицу cr_daily_stats.
    
    Args:
        records: Список записей для upsert
        supabase: Клиент Supabase
        label: Метка для логирования (например, "сегодня" или "вчера")
    
    Returns:
        Количество обработанных записей
    """
    if not records:
        print(f"⚠️  Нет записей для upsert ({label})")
        return 0
    
    print(f"🔄 Upsert в Supabase ({label})...")
    
    try:
        response = supabase.table('cr_daily_stats').upsert(
            records,
            on_conflict='nm_id,date_of_period'
        ).execute()
        
        count = len(response.data) if response.data else len(records)
        print(f"✅ Обработано записей ({label}): {count}")
        return count
    
    except Exception as e:
        print(f"❌ ОШИБКА при upsert ({label}): {e}")
        raise

