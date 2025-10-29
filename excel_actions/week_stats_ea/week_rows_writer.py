"""Week rows writer for inserting detailed report rows into week_rows table."""

from typing import Dict, Any, List, Tuple
from supabase import Client


def normalize_number(value):
    """Convert empty strings or None to None for BIGINT fields."""
    if value is None or value == '' or value == 'None':
        return None
    try:
        return int(value) if isinstance(value, (int, float)) else None
    except (ValueError, TypeError):
        return None


def get_products_map(supabase: Client) -> Dict[int, str]:
    """Get mapping of nm_id -> product_id from products table."""
    response = supabase.table('products').select('id, nm_id').execute()
    return {row['nm_id']: row['id'] for row in response.data if row.get('nm_id')}


def calculate_aggregated_fields(record: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate aggregated fields for a single record."""
    retail_price = record.get('retail_price', 0) or 0
    retail_amount = record.get('retail_amount', 0) or 0
    ppvz_for_pay = record.get('ppvz_for_pay', 0) or 0
    ppvz_spp_prc = record.get('ppvz_spp_prc', 0) or 0
    
    # rub_discountWB_both
    rub_discountwb_both = retail_price - retail_amount
    
    # perc_discountWB_both
    perc_discountwb_both = (rub_discountwb_both / retail_price * 100) if retail_price != 0 else 0
    
    # rub_spp
    rub_spp = (retail_amount * ppvz_spp_prc / 100) if ppvz_spp_prc else 0
    
    # rub_wallet_dicount
    rub_wallet_dicount = record.get('rub_wallet_dicount', 0) or 0
    
    # perc_wallet_discount
    perc_wallet_discount = record.get('perc_wallet_discount', 0) or 0
    
    # rub_commision_both
    rub_commision_both = retail_price - ppvz_for_pay
    
    # perc_commisian_both
    perc_commisian_both = (rub_commision_both / retail_price * 100) if retail_price != 0 else 0
    
    # rub_commission
    commission_percent = record.get('commission_percent', 0) or 0
    rub_commission = (retail_amount * commission_percent / 100) if commission_percent else 0
    
    # rub_excess_comission
    rub_excess_comission = record.get('rub_excess_comission', 0) or 0
    
    # perc_excess_comission
    perc_excess_comission = record.get('perc_excess_comission', 0) or 0
    
    return {
        'rub_discountwb_both': rub_discountwb_both,
        'perc_discountwb_both': perc_discountwb_both,
        'rub_spp': rub_spp,
        'rub_wallet_dicount': rub_wallet_dicount,
        'perc_wallet_discount': perc_wallet_discount,
        'rub_commision_both': rub_commision_both,
        'perc_commisian_both': perc_commisian_both,
        'rub_commission': rub_commission,
        'rub_excess_comission': rub_excess_comission,
        'perc_excess_comission': perc_excess_comission,
    }


def insert_week_rows(
    records: List[Dict[str, Any]],
    supabase: Client,
    realizationreport_id: int
) -> Tuple[int, List[int]]:
    """
    Insert week_rows into database.
    
    Args:
        records: List of detailed report records
        supabase: Supabase client
        realizationreport_id: Report ID
        
    Returns:
        Tuple[int, List[int]]: (количество вставленных записей, список отсутствующих nm_id)
    """
    if not records:
        return 0, []
    
    # Получаем маппинг nm_id -> product_id
    products_map = get_products_map(supabase)
    
    # Получаем существующие rr_id для этого отчета
    existing_rows = supabase.table('week_rows').select('rr_id').eq('realizationreport_id', realizationreport_id).execute()
    existing_rr_ids = set([row.get('rr_id') for row in existing_rows.data if row.get('rr_id') is not None])
    
    # Фильтруем записи: пропускаем те, где отсутствует nm_id (None, 0, пустое значение)
    filtered_records = []
    skipped_no_nm_id = 0
    for record in records:
        nm_id = record.get('nm_id')
        # Пропускаем записи без nm_id (None, 0, пустое)
        if not nm_id or nm_id == 0 or nm_id == '' or nm_id == 'None':
            skipped_no_nm_id += 1
            continue
        filtered_records.append(record)
    
    print(f"ℹ️  Отфильтровано записей без nm_id: {skipped_no_nm_id}")
    print(f"ℹ️  Записей с nm_id для обработки: {len(filtered_records)}")
    
    # Формируем записи для вставки
    rows_to_insert = []
    missing_nm_ids_set = set()  # Используем set для избежания дублей
    skipped_existing = 0
    skipped_missing_nm_id_count = 0  # Счетчик строк, отфильтрованных из-за отсутствующих nm_id
    
    for record in filtered_records:
        rr_id_raw = record.get('rrd_id')
        rr_id = normalize_number(rr_id_raw)
        
        # Пропускаем, если rr_id пустой
        if rr_id is None:
            continue
        
        # Пропускаем, если строка уже есть в БД
        if rr_id in existing_rr_ids:
            skipped_existing += 1
            continue
        
        nm_id = record.get('nm_id')
        
        # Определяем product_id
        product_id = None
        if nm_id in products_map:
            product_id = products_map[nm_id]
        else:
            # nm_id есть, но не найден в products - добавляем в set и считаем строку
            if nm_id not in missing_nm_ids_set:
                missing_nm_ids_set.add(nm_id)
            skipped_missing_nm_id_count += 1
            continue
        
        # Вычисляем агрегированные поля
        aggregated = calculate_aggregated_fields(record)
        
        # Формируем строку для вставки
        row = {
            'product_id': product_id,
            'realizationreport_id': realizationreport_id,
            'report_type': record.get('report_type'),
            'rr_id': rr_id,
            'gi_id': normalize_number(record.get('gi_id')),
            'order_dt': record.get('order_dt'),
            'sale_dt': record.get('sale_dt'),
            'srid': record.get('srid'),
            'barcode': normalize_number(record.get('barcode')),
            'ts_name': record.get('ts_name'),
            'nm_id': normalize_number(nm_id),
            'sa_name': record.get('sa_name'),
            'doc_type_name': record.get('doc_type_name'),
            'quantity': record.get('quantity'),
            'retail_price': record.get('retail_price'),
            'retail_amount': record.get('retail_amount'),
            'rub_discountwb_both': aggregated.get('rub_discountwb_both'),
            'perc_discountwb_both': aggregated.get('perc_discountwb_both'),
            'ppvz_spp_prc': record.get('ppvz_spp_prc'),
            'rub_spp': aggregated.get('rub_spp'),
            'rub_wallet_dicount': aggregated.get('rub_wallet_dicount'),
            'perc_wallet_discount': aggregated.get('perc_wallet_discount'),
            'ppvz_for_pay': record.get('ppvz_for_pay'),
            'rub_commision_both': aggregated.get('rub_commision_both'),
            'perc_commisian_both': aggregated.get('perc_commisian_both'),
            'commission_percent': record.get('commission_percent'),
            'rub_commission': aggregated.get('rub_commission'),
            'rub_excess_comission': aggregated.get('rub_excess_comission'),
            'perc_excess_comission': aggregated.get('perc_excess_comission'),
            'supplier_oper_name': record.get('supplier_oper_name'),
            'bonus_type_name': record.get('bonus_type_name'),
            'delivery_amount': record.get('delivery_amount'),
            'return_amount': record.get('return_amount'),
            'delivery_rub': record.get('delivery_rub'),
            'site_country': record.get('site_country'),
            'office_name': record.get('office_name'),
            'penalty': record.get('penalty'),
            'storage_fee': record.get('storage_fee'),
            'deduction': record.get('deduction'),
            'acceptance': record.get('acceptance'),
            'kiz': record.get('kiz'),
        }
        
        rows_to_insert.append(row)
    
    # Преобразуем set в отсортированный list
    missing_nm_ids_list = sorted(list(missing_nm_ids_set))
    
    # Логируем отсутствующие nm_id
    if missing_nm_ids_list:
        print(f"⚠️  Отсутствуют в products ({len(missing_nm_ids_list)} nm_id): {missing_nm_ids_list}")
    
    # Выводим статистику по фильтрации
    print(f"📊 Статистика фильтрации:")
    print(f"   Отфильтровано из-за отсутствующих в products: {skipped_missing_nm_id_count} строк")
    print(f"   Отфильтровано уже существующих в БД: {skipped_existing} строк")
    
    if not rows_to_insert:
        return 0, missing_nm_ids_list
    
    # Логируем результат вставки
    print(f"✅ Вставлено строк в week_rows: {len(rows_to_insert)}")
    
    # Вставляем записи
    try:
        response = supabase.table('week_rows').insert(rows_to_insert).execute()
        count = len(response.data) if response.data else len(rows_to_insert)
        return count, missing_nm_ids_list
    except Exception as e:
        print(f"❌ ОШИБКА при insert в week_rows: {e}")
        raise
