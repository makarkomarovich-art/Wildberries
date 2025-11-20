"""Week rows writer for inserting detailed report rows into week_rows table."""

from typing import Dict, Any, List, Tuple
from supabase import Client


def normalize_number(value):
    """Convert empty strings or None to None for BIGINT fields."""
    if value is None or value == '' or value == 'None':
        return None
    try:
        # Поддерживаем int/float/строки вида "123" или "123.0"
        return int(float(str(value)))
    except (ValueError, TypeError):
        return None


def normalize_size(value: Any) -> str | None:
    """Нормализация значения размера для сопоставления."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s.lower()


def get_product_sizes_maps(supabase: Client) -> Tuple[Dict[int, Dict[str, Any]], Dict[Tuple[int, str], Dict[str, Any]], Dict[int, List[Dict[str, Any]]]]:
    """
    Загружаем product_sizes и строим быстрые мапы:
      - by_barcode: numeric_barcode -> row
      - by_nm_and_size: (nm_id, size_norm) -> row
      - by_nm: nm_id -> [row, ...]
    """
    response = supabase.table('product_sizes').select('id, nm_id, size, barcode').execute()
    by_barcode: Dict[int, Dict[str, Any]] = {}
    by_nm_and_size: Dict[Tuple[int, str], Dict[str, Any]] = {}
    by_nm: Dict[int, List[Dict[str, Any]]] = {}

    for row in response.data or []:
        nm_id = row.get('nm_id')
        size = row.get('size')
        barcode_raw = row.get('barcode')
        numeric_barcode = normalize_number(barcode_raw)

        if numeric_barcode is not None:
            # Если вдруг дубликаты по barcode - оставляем первый попавшийся
            by_barcode.setdefault(numeric_barcode, row)

        if nm_id is not None:
            nm_int = int(nm_id)
            size_norm = normalize_size(size)
            if size_norm is not None:
                by_nm_and_size[(nm_int, size_norm)] = row
            by_nm.setdefault(nm_int, []).append(row)

    return by_barcode, by_nm_and_size, by_nm


def correct_barcode_and_product_size(
    record: Dict[str, Any],
    by_barcode: Dict[int, Dict[str, Any]],
    by_nm_and_size: Dict[Tuple[int, str], Dict[str, Any]],
    by_nm: Dict[int, List[Dict[str, Any]]],
    counters: Dict[str, Any],
) -> Tuple[int | None, str | None]:
    """
    Корректируем barcode и подбираем product_size_id по правилам:
      1) прямое попадание по barcode
      2) поиск по (nm_id, size)
      3) поиск по nm_id
      4) иначе оставляем как есть (barcode может быть None/кривым, product_size_id = None)
    """
    nm_id_raw = record.get('nm_id')
    nm_int = normalize_number(nm_id_raw)
    size_raw = record.get('ts_name')
    size_norm = normalize_size(size_raw)

    barcode_raw = record.get('barcode')
    numeric_barcode = normalize_number(barcode_raw)

    # Шаг 1: прямое попадание по barcode
    row = by_barcode.get(numeric_barcode) if numeric_barcode is not None else None
    if row is not None:
        product_size_id = row.get('id')
        return numeric_barcode, product_size_id

    # Сюда попали либо с пустым/кривым barcode, либо barcode не найден в product_sizes
    counters['invalid_barcode_found'] += 1

    # Шаг 2: поиск по (nm_id, size)
    if nm_int is not None and size_norm is not None:
        row = by_nm_and_size.get((nm_int, size_norm))
        if row is not None:
            corrected_barcode = normalize_number(row.get('barcode'))
            product_size_id = row.get('id')
            counters['corrected_by_nm_and_size'] += 1
            return corrected_barcode, product_size_id

    # Шаг 3: поиск по nm_id
    if nm_int is not None:
        rows_for_nm = by_nm.get(nm_int) or []
        if rows_for_nm:
            # Берём первый вариант по этому nm_id
            row = rows_for_nm[0]
            corrected_barcode = normalize_number(row.get('barcode'))
            product_size_id = row.get('id')
            counters['corrected_by_nm_only'] += 1
            return corrected_barcode, product_size_id

    # Шаг 4: ничего не нашли — считаем нерешённым
    counters['unresolved_barcodes'] += 1
    # Для логирования сохраняем сырой barcode
    raw_str = str(barcode_raw)
    counters['unresolved_barcodes_samples'].add(raw_str)
    return numeric_barcode, None


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
    
    # rub_spp: рассчитываем как ppvz_spp_prc * retail_price / 100
    rub_spp = (ppvz_spp_prc * retail_price / 100) if ppvz_spp_prc else 0
    
    # rub_wallet_dicount = скидка WB - СПП
    rub_wallet_dicount = rub_discountwb_both - rub_spp
    
    # perc_wallet_discount = rub_wallet_dicount / retail_price * 100
    perc_wallet_discount = (rub_wallet_dicount / retail_price * 100) if retail_price != 0 else 0
    
    # rub_commision_both
    rub_commision_both = retail_price - ppvz_for_pay
    
    # perc_commisian_both
    perc_commisian_both = (rub_commision_both / retail_price * 100) if retail_price != 0 else 0
    
    # rub_commission: комиссия WB по ставке комиссии (commission_percent * retail_price / 100)
    commission_percent = record.get('commission_percent', 0) or 0
    rub_commission = (retail_price * commission_percent / 100) if commission_percent else 0
    
    # rub_excess_comission = rub_commision_both - rub_commission
    rub_excess_comission = rub_commision_both - rub_commission
    
    # perc_excess_comission = rub_excess_comission / retail_price * 100
    perc_excess_comission = (rub_excess_comission / retail_price * 100) if retail_price != 0 else 0
    
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
    
    # Загружаем product_sizes и подготавливаем мапы для коррекции barcode
    by_barcode, by_nm_and_size, by_nm = get_product_sizes_maps(supabase)
    
    # Получаем существующие rr_id для этого отчета
    existing_rows = supabase.table('week_rows').select('rr_id').eq('realizationreport_id', realizationreport_id).execute()
    existing_rr_ids = set([row.get('rr_id') for row in existing_rows.data if row.get('rr_id') is not None])
    
    # Счётчики и коллекции для логирования
    removed_by_oper_name = 0
    skipped_existing = 0
    total_rows_input = len(records)

    counters = {
        'invalid_barcode_found': 0,
        'corrected_by_nm_and_size': 0,
        'corrected_by_nm_only': 0,
        'unresolved_barcodes': 0,
        'unresolved_barcodes_samples': set(),
    }
    missing_nm_ids_set = set()

    rows_to_insert = []

    for record in records:
        # Сначала фильтруем по supplier_oper_name
        oper_name = (record.get('supplier_oper_name') or '').strip()
        if oper_name in [
            "Возмещение издержек по перевозке/по складским операциям с товаром",
            "Компенсация скидки по программе лояльности",
        ]:
            removed_by_oper_name += 1
            continue

        rr_id_raw = record.get('rrd_id')
        rr_id = normalize_number(rr_id_raw)
        
        # Пропускаем, если rr_id пустой
        if rr_id is None:
            continue
        
        # Пропускаем, если строка уже есть в БД
        if rr_id in existing_rr_ids:
            skipped_existing += 1
            continue

        # Корректируем barcode и подбираем product_size_id
        corrected_barcode, product_size_id = correct_barcode_and_product_size(
            record,
            by_barcode,
            by_nm_and_size,
            by_nm,
            counters,
        )

        # Собираем список nm_id, для которых так и не нашли product_size
        if product_size_id is None:
            nm_val = record.get('nm_id')
            if nm_val is not None:
                try:
                    missing_nm_ids_set.add(int(nm_val))
                except (TypeError, ValueError):
                    pass
        
        # Вычисляем агрегированные поля
        aggregated = calculate_aggregated_fields(record)
        
        # Формируем строку для вставки
        row = {
            'product_size_id': product_size_id,
            'realizationreport_id': realizationreport_id,
            'report_type': record.get('report_type'),
            'rr_id': rr_id,
            'gi_id': normalize_number(record.get('gi_id')),
            'order_dt': record.get('order_dt'),
            'sale_dt': record.get('sale_dt'),
            'srid': record.get('srid'),
            'barcode': corrected_barcode,
            'ts_name': record.get('ts_name'),
            'nm_id': normalize_number(record.get('nm_id')),
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
    
    # Логирование сводной статистики
    print("📊 Статистика обработки week_rows:")
    print(f"   Всего строк на входе: {total_rows_input}")
    print(f"   Удалено по supplier_oper_name (компенсации): {removed_by_oper_name}")
    print(f"   Отфильтровано уже существующих в БД по rr_id: {skipped_existing}")
    print(f"   Строк с исходно левым/пустым barcode: {counters['invalid_barcode_found']}")
    print(f"   Скорректировано по nm_id + size: {counters['corrected_by_nm_and_size']}")
    print(f"   Скорректировано только по nm_id: {counters['corrected_by_nm_only']}")
    print(f"   Нерешённых barcodes (остались как есть, включая NULL): {counters['unresolved_barcodes']}")
    if counters['unresolved_barcodes']:
        samples = list(counters['unresolved_barcodes_samples'])
        print(f"   Примеры нерешённых barcodes (макс 20): {samples[:20]}")
    if missing_nm_ids_list:
        print(f"⚠️  Для этих nm_id не найдено product_sizes ({len(missing_nm_ids_list)} nm_id): {missing_nm_ids_list}")
    
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
