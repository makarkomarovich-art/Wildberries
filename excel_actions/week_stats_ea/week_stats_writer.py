from typing import Dict, Any, List, Tuple, Optional
from supabase import Client


def get_week_report_dates_and_type(supabase: Client, realizationreport_id: int) -> Tuple[str, str, str]:
    resp = supabase.table('week_reports').select('date_from, date_to, report_type').eq('realizationreport_id', realizationreport_id).single().execute()
    row = resp.data or {}
    return row.get('date_from'), row.get('date_to'), row.get('report_type')


def aggregate_week_rows_by_barcode(rows: List[Dict[str, Any]]) -> Dict[Optional[int], Dict[str, Any]]:
    """
    Агрегируем строки week_rows по barcode.
    Для каждого barcode (включая NULL) создаём запись с накопленными суммами.
    """
    agg: Dict[Optional[int], Dict[str, Any]] = {}
    
    for r in rows:
        barcode = r.get('barcode')  # может быть None/NULL или int
        
        # Для уникальности используем barcode как ключ (включая None)
        a = agg.setdefault(barcode, {
            'nm_id': None,
            'size': None,
            'product_size_id': None,
            'sa_name': None,
            'quantity_sells_nm': 0,
            'quantity_return_nm': 0,
            'retail_price_nm': 0.0,
            'retail_amount_nm': 0.0,
            'rub_spp_nm': 0.0,
            'perc_wallet_discount_nm': 0.0,
            'ppvz_for_pay_nm': 0.0,
            'rub_commission_nm': 0.0,
            'delivery_amount_nm': 0,
            'return_amount_nm': 0,
            'delivery_rub_nm': 0.0,
        })
        
        # Запоминаем nm_id, size, product_size_id из первой встретившейся строки
        if a['nm_id'] is None:
            a['nm_id'] = r.get('nm_id')
        if a['size'] is None:
            a['size'] = r.get('ts_name')
        if a['product_size_id'] is None:
            a['product_size_id'] = r.get('product_size_id')
        if a['sa_name'] is None:
            a['sa_name'] = r.get('sa_name')
        
        oper = r.get('supplier_oper_name') or ''
        qty = int(r.get('quantity') or 0)
        price = float(r.get('retail_price') or 0)
        amount = float(r.get('retail_amount') or 0)
        spp_prc = float(r.get('ppvz_spp_prc') or 0)
        ppvz = float(r.get('ppvz_for_pay') or 0)
        comm_prc = float(r.get('commission_percent') or 0)
        delivery_amt_cnt = int(r.get('delivery_amount') or 0)
        return_amt_cnt = int(r.get('return_amount') or 0)
        delivery_rub = float(r.get('delivery_rub') or 0)

        if oper in ['Продажа', 'Возврат']:
            # Продажа / Возврат учитываем со знаком
            mult = 1 if oper == 'Продажа' else -1
            a['quantity_sells_nm'] += qty * mult
            a['retail_price_nm'] += price * mult
            a['retail_amount_nm'] += amount * mult
            a['ppvz_for_pay_nm'] += ppvz * mult
            # rub_commission_nm: commission_percent * retail_price / 100 с учетом знака
            if comm_prc:
                a['rub_commission_nm'] += mult * (price * comm_prc / 100.0)
            # rub_spp_nm: ppvz_spp_prc * retail_price / 100 с учетом знака
            if spp_prc:
                a['rub_spp_nm'] += mult * (price * spp_prc / 100.0)
        if oper == 'Возврат':
            a['quantity_return_nm'] += qty
        if oper == 'Логистика':
            a['delivery_amount_nm'] += delivery_amt_cnt
            a['return_amount_nm'] += return_amt_cnt
            a['delivery_rub_nm'] += delivery_rub

    # Вычисляем производные поля для каждого barcode
    for barcode_key, a in agg.items():
        a['cancels_nm'] = int(a['return_amount_nm'] - a['quantity_return_nm'])
        a['rub_discountwb_both_nm'] = a['retail_price_nm'] - a['retail_amount_nm']
        a['rub_commision_both_nm'] = a['retail_price_nm'] - a['ppvz_for_pay_nm']
        a['rub_excess_comission_nm'] = a['rub_commision_both_nm'] - a['rub_commission_nm']
        denom = a['retail_price_nm'] if a['retail_price_nm'] != 0 else 1.0
        a['perc_discountwb_both_nm'] = (a['rub_discountwb_both_nm'] / denom) * 100.0
        a['perc_spp_nm'] = (a['rub_spp_nm'] / denom) * 100.0
        a['perc_wallet_discount_nm'] = ((a['rub_discountwb_both_nm'] - a['rub_spp_nm']) / denom) * 100.0
        a['perc_commisian_both_nm'] = (a['rub_commision_both_nm'] / denom) * 100.0
        a['perc_commission_nm'] = (a['rub_commission_nm'] / denom) * 100.0
        a['perc_excess_comission_nm'] = (a['rub_excess_comission_nm'] / denom) * 100.0
    
    return agg


def load_rows_from_db(supabase: Client, realizationreport_id: int) -> List[Dict[str, Any]]:
    """
    Загружаем все строки week_rows для данного отчёта.
    Теперь НЕ фильтруем по barcode (разрешаем NULL).
    """
    resp = supabase.table('week_rows').select('*').eq('realizationreport_id', realizationreport_id).execute()
    return resp.data or []


def insert_week_stats_for_report(
    supabase: Client,
    realizationreport_id: int
) -> Tuple[int, int]:
    """
    Загружаем week_rows, агрегируем по barcode, вставляем в week_stats.
    Возвращаем (кол-во вставленных записей, общее кол-во уникальных barcode).
    """
    date_from, date_to, report_type = get_week_report_dates_and_type(supabase, realizationreport_id)

    db_rows = load_rows_from_db(supabase, realizationreport_id)
    agg = aggregate_week_rows_by_barcode(db_rows)

    candidates: List[Dict[str, Any]] = []
    for barcode, a in agg.items():
        rec = {
            'report_type': report_type,
            'realizationreport_id': realizationreport_id,
            'product_size_id': a.get('product_size_id'),
            'date_from': date_from,
            'date_to': date_to,
            'nm_id': a.get('nm_id'),
            'barcode': barcode,
            'ts_code': a.get('size'),
            'sa_name': a.get('sa_name'),
            'quantity_sells_nm': int(a.get('quantity_sells_nm') or 0),
            'quantity_return_nm': int(a.get('quantity_return_nm') or 0),
            'cancels_nm': int(a.get('cancels_nm') or 0),
            'retail_price_nm': a.get('retail_price_nm'),
            'retail_amount_nm': a.get('retail_amount_nm'),
            'rub_discountwb_both_nm': a.get('rub_discountwb_both_nm'),
            'perc_discountwb_both_nm': a.get('perc_discountwb_both_nm'),
            'rub_spp_nm': a.get('rub_spp_nm'),
            'perc_spp_nm': a.get('perc_spp_nm'),
            'perc_wallet_discount_nm': a.get('perc_wallet_discount_nm'),
            'ppvz_for_pay_nm': a.get('ppvz_for_pay_nm'),
            'rub_commision_both_nm': a.get('rub_commision_both_nm'),
            'perc_commisian_both_nm': a.get('perc_commisian_both_nm'),
            'rub_commission_nm': a.get('rub_commission_nm'),
            'perc_commission_nm': a.get('perc_commission_nm'),
            'rub_excess_comission_nm': a.get('rub_excess_comission_nm'),
            'perc_excess_comission_nm': a.get('perc_excess_comission_nm'),
            'delivery_amount_nm': a.get('delivery_amount_nm'),
            'return_amount_nm': a.get('return_amount_nm'),
            'delivery_rub_nm': a.get('delivery_rub_nm'),
        }
        candidates.append(rec)

    # Проверяем существующие записи по (realizationreport_id, barcode)
    existing_resp = supabase.table('week_stats').select('realizationreport_id, barcode').eq('realizationreport_id', realizationreport_id).execute()
    existing_pairs = {(row['realizationreport_id'], row.get('barcode')) for row in (existing_resp.data or [])}

    to_insert = [rec for rec in candidates if (rec['realizationreport_id'], rec['barcode']) not in existing_pairs]

    total_unique_barcodes = len(agg.keys())
    will_insert = len(to_insert)

    if not to_insert:
        return 0, total_unique_barcodes

    resp = supabase.table('week_stats').insert(to_insert).execute()
    inserted = len(resp.data) if resp.data else len(to_insert)
    return inserted, total_unique_barcodes


def verify_week_stats_totals(
    supabase: Client,
    realizationreport_id: int
) -> None:
    """
    Сверяем агрегированные суммы из week_stats с итоговыми значениями в week_reports.
    """
    ws = supabase.table('week_stats').select('retail_amount_nm, ppvz_for_pay_nm, delivery_rub_nm').eq('realizationreport_id', realizationreport_id).execute()
    tot_amount = sum(float(r.get('retail_amount_nm') or 0) for r in (ws.data or []))
    tot_ppvz = sum(float(r.get('ppvz_for_pay_nm') or 0) for r in (ws.data or []))
    tot_delivery = sum(float(r.get('delivery_rub_nm') or 0) for r in (ws.data or []))

    wr = supabase.table('week_reports').select('retail_amount_total, ppvz_for_pay_total, delivery_rub_total').eq('realizationreport_id', realizationreport_id).single().execute()
    wr_row = wr.data or {}
    wr_amount = float(wr_row.get('retail_amount_total') or 0)
    wr_ppvz = float(wr_row.get('ppvz_for_pay_total') or 0)
    wr_delivery = float(wr_row.get('delivery_rub_total') or 0)

    ok = (round(tot_amount, 2) == round(wr_amount, 2) and round(tot_ppvz, 2) == round(wr_ppvz, 2) and round(tot_delivery, 2) == round(wr_delivery, 2))
    if ok:
        print(f"✅ Суммы из week_stats совпадают с week_reports для отчета {realizationreport_id}")
    else:
        print(f"⚠️ Несовпадение сумм для отчета {realizationreport_id}. Проверьте вручную")
