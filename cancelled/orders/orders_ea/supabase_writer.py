"""
Модуль для записи заказов в Supabase.
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List, Set
from supabase import Client

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import api_keys


def enrich_with_product_ids(records: List[Dict], supabase: Client) -> List[Dict]:
    """
    Обогащает записи product_id из таблицы products по nm_id.
    
    Args:
        records: Список заказов
        supabase: Клиент Supabase
        
    Returns:
        list: Записи с добавленным product_id
    """
    print(f"🔄 Получение product_id из таблицы products...")
    
    # Получаем все products из БД
    response = supabase.table('products').select('nm_id, id').execute()
    products_map = {p['nm_id']: p['id'] for p in response.data}
    
    print(f"✅ Загружено products: {len(products_map)}")
    
    # Обогащаем записи
    enriched_records = []
    skipped_count = 0
    skipped_nm_ids: Set[int] = set()
    
    for record in records:
        nm_id = record.get('nm_id')
        if not nm_id:
            continue
        product_id = products_map.get(nm_id)
        
        if not product_id:
            skipped_nm_ids.add(nm_id)
            skipped_count += 1
            continue
        
        record['product_id'] = product_id
        enriched_records.append(record)
    
    if skipped_count > 0:
        print(f"⚠️  Пропущено {skipped_count} записей (нет product_id): {list(skipped_nm_ids)[:10]}")
    print(f"✅ Обогащено: {len(enriched_records)} из {len(records)}")
    
    return enriched_records


def enrich_with_delivery_expr(records: List[Dict], supabase: Client) -> List[Dict]:
    """
    Обогащает записи delivery_and_storage_expr из supplies_to_warehouses.
    
    Args:
        records: Список заказов с product_id
        supabase: Клиент Supabase
        
    Returns:
        list: Записи с добавленным delivery_and_storage_expr
    """
    print(f"🔄 Получение delivery_and_storage_expr из supplies_to_warehouses...")
    
    # Получаем ВСЕ supplies
    response = supabase.table('supplies_to_warehouses').select('income_id, delivery_and_storage_expr').execute()
    
    # Строим два словаря
    delivery_map = {}
    all_income_ids = set()
    
    for s in response.data:
        income_id = s['income_id']
        all_income_ids.add(income_id)
        delivery_expr = s['delivery_and_storage_expr']
        if delivery_expr is not None:
            delivery_map[income_id] = delivery_expr
    
    print(f"✅ Загружено supplies: {len(all_income_ids)}")
    
    # Обогащаем записи
    enriched_records = []
    not_found_count = 0
    null_expr_count = 0
    empty_income_id_count = 0
    not_found_income_ids: Set[int] = set()
    
    for record in records:
        income_id = record.get('income_id')
        if not income_id:
            empty_income_id_count += 1
            continue
        
        if income_id not in all_income_ids:
            not_found_income_ids.add(income_id)
            not_found_count += 1
            continue
        elif income_id not in delivery_map:
            record['delivery_and_storage_expr'] = 150
            null_expr_count += 1
            enriched_records.append(record)
        else:
            record['delivery_and_storage_expr'] = delivery_map[income_id]
            enriched_records.append(record)
    
    if empty_income_id_count > 0:
        print(f"⚠️  Пропущено {empty_income_id_count} записей (income_id пустой)")
    if not_found_count > 0:
        print(f"⚠️  Пропущено {not_found_count} записей (incomeID не найден): {sorted(list(not_found_income_ids))}")
    if null_expr_count > 0:
        print(f"ℹ️  Установлено delivery_expr=150 для {null_expr_count} записей (был NULL)")
    
    print(f"✅ Обогащено: {len(enriched_records)} из {len(records)}")
    
    return enriched_records


def transform_orders(orders: List[Dict]) -> List[Dict]:
    """
    Преобразует данные заказов из API в формат БД.
    
    Args:
        orders: Заказы из API
        
    Returns:
        list: Трансформированные заказы
    """
    transformed = []
    
    for order in orders:
        transformed_order = {
            'srid': str(order['srid']),
            'date': order['date'],
            'last_change_date': order['lastChangeDate'],
            'warehouse_name': order['warehouseName'],
            'warehouse_type': order.get('warehouseType'),
            'country_name': order.get('countryName'),
            'oblast_okrug_name': order.get('oblastOkrugName'),
            'region_name': order.get('regionName'),
            'nm_id': order['nmId'],
            'barcode': order['barcode'],
            'supplier_article': order['supplierArticle'],
            'tech_size': order.get('techSize'),
            'category': order.get('category'),
            'subject': order.get('subject'),
            'brand': order.get('brand'),
            'income_id': order['incomeID'],
            'is_supply': order.get('isSupply', False),
            'is_realization': order.get('isRealization', False),
            'is_cancel': order.get('isCancel', False),
            'total_price': order.get('totalPrice', 0),
            'discount_percent': order.get('discountPercent', 0),
            'spp': order.get('spp', 0),
            'finished_price': order.get('finishedPrice', 0),
            'price_with_disc': order.get('priceWithDisc', 0),
            'cancel_date': order.get('cancelDate') if order.get('cancelDate') != '0001-01-01T00:00:00' else None,
            'sticker': order.get('sticker'),
            'g_number': order.get('gNumber'),
        }
        
        transformed.append(transformed_order)
    
    return transformed


def get_existing_orders(supabase: Client) -> Set[str]:
    """
    Получает список существующих srid из БД.
    
    Args:
        supabase: Клиент Supabase
        
    Returns:
        set: Множество существующих srid
    """
    try:
        response = supabase.table("orders").select("srid").execute()
        return {record["srid"] for record in response.data}
    except Exception as e:
        print(f"❌ Ошибка загрузки существующих заказов: {e}")
        return set()


def upsert_orders(records: List[Dict], supabase: Client, batch_size: int = 1000) -> int:
    """
    Записывает заказы в БД с использованием UPSERT.
    
    Args:
        records: Трансформированные заказы
        supabase: Клиент Supabase
        batch_size: Размер батча для вставки
        
    Returns:
        int: Количество записанных записей
    """
    if not records:
        return 0
    
    written_count = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        try:
            response = supabase.table("orders").upsert(
                batch,
                on_conflict="srid"
            ).execute()
            
            written_count += len(response.data)
            
        except Exception as e:
            print(f"❌ Ошибка при записи: {e}")
            raise
    
    print(f"✅ Записано {written_count} из {len(records)}")
    return written_count


def write_orders_to_supabase(orders: List[Dict], supabase: Client) -> int:
    """
    Главная функция для записи заказов в Supabase.
    
    Args:
        orders: Заказы из API
        supabase: Клиент Supabase
        
    Returns:
        int: Количество записанных записей
    """
    print("\n" + "=" * 60)
    print("📥 ЗАПИСЬ ЗАКАЗОВ В SUPABASE")
    print("=" * 60 + "\n")
    
    transformed = transform_orders(orders)
    enriched_with_products = enrich_with_product_ids(transformed, supabase)
    
    if not enriched_with_products:
        return 0
    
    enriched_full = enrich_with_delivery_expr(enriched_with_products, supabase)
    
    if not enriched_full:
        return 0
    
    existing_srids = get_existing_orders(supabase)
    new_records = [r for r in enriched_full if r['srid'] not in existing_srids]
    
    if not new_records:
        print(f"✅ Все заказы уже в БД ({len(existing_srids)})")
        return 0
    
    written = upsert_orders(new_records, supabase)
    
    return written
