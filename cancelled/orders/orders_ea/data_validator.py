"""
Валидатор данных для orders.
Проверяет корректность записанных в БД данных.
"""

from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from supabase import Client
import api_keys


def validate_orders_in_db(
    orders: List[Dict[str, Any]], 
    supabase: Client,
    sample_size: int = 50
) -> Dict[str, Any]:
    """
    Валидирует что заказы корректно записаны в БД.
    
    Args:
        orders: Исходные заказы из API
        supabase: Клиент Supabase
        sample_size: Размер выборки для проверки
        
    Returns:
        Dict с результатами проверки
    """
    print("🔍 Проверка целостности данных в БД...")
    
    # Берем выборку для проверки
    sample_orders = orders[:sample_size] if len(orders) > sample_size else orders
    
    # Получаем srid для выборки
    sample_srids = [str(o['srid']) for o in sample_orders]
    
    # Загружаем из БД
    try:
        response = supabase.table("orders").select("*").in_("srid", sample_srids).execute()
        db_orders = {o['srid']: o for o in response.data}
    except Exception as e:
        print(f"❌ Ошибка загрузки из БД: {e}")
        return {"valid": False, "error": str(e)}
    
    # Проверяем
    validated_count = 0
    missing_count = 0
    error_details = []
    
    for order in sample_orders:
        srid = str(order['srid'])
        
        if srid not in db_orders:
            missing_count += 1
            error_details.append(f"  - {srid}: не найден в БД")
            continue
        
        db_order = db_orders[srid]
        
        # Проверяем ключевые поля
        checks = [
            (order['nmId'], db_order.get('nm_id'), 'nm_id'),
            (order['barcode'], db_order.get('barcode'), 'barcode'),
            (order['supplierArticle'], db_order.get('supplier_article'), 'supplier_article'),
            (order['warehouseName'], db_order.get('warehouse_name'), 'warehouse_name'),
        ]
        
        for api_val, db_val, field in checks:
            if api_val != db_val:
                error_details.append(f"  - {srid}.{field}: API={api_val}, БД={db_val}")
        
        validated_count += 1
    
    result = {
        "valid": missing_count == 0 and len(error_details) == 0,
        "sample_size": len(sample_orders),
        "validated": validated_count,
        "missing": missing_count,
        "errors": error_details[:10]  # Первые 10 ошибок
    }
    
    if result["valid"]:
        print(f"✅ Проверено записей: {validated_count}/{len(sample_orders)}")
    else:
        print(f"⚠️  Найдено проблем: {missing_count} не найдено, {len(error_details)} ошибок")
        if error_details:
            print("Примеры ошибок:")
            for err in error_details[:5]:
                print(err)
    
    return result


def validate_orders_count(supabase: Client) -> Dict[str, Any]:
    """
    Проверяет общее количество заказов в БД.
    
    Args:
        supabase: Клиент Supabase
        
    Returns:
        Dict с результатами
    """
    try:
        response = supabase.table("orders").select("id", count="exact").execute()
        count = response.count or 0
        print(f"📊 Заказов в БД: {count}")
        return {"count": count, "valid": True}
    except Exception as e:
        print(f"❌ Ошибка проверки количества: {e}")
        return {"count": 0, "valid": False, "error": str(e)}
