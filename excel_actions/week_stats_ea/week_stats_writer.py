from typing import Dict, Any, List, Tuple
from supabase import Client


def get_week_report_dates_and_type(supabase: Client, realizationreport_id: int) -> Tuple[str, str, str]:
    resp = supabase.table('week_reports').select('date_from, date_to, report_type').eq('realizationreport_id', realizationreport_id).single().execute()
    row = resp.data or {}
    return row.get('date_from'), row.get('date_to'), row.get('report_type')


def get_products_map(supabase: Client) -> Dict[int, str]:
    resp = supabase.table('products').select('id, nm_id').execute()
    return {r['nm_id']: r['id'] for r in resp.data if r.get('nm_id')}


def aggregate_week_rows_by_nm(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    agg: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        nm = r.get('nm_id')
        if nm is None:
            continue
        a = agg.setdefault(nm, {
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

        if a['sa_name'] is None:
            a['sa_name'] = r.get('sa_name')

        if oper in ['Продажа', 'Возврат']:
            mult = 1 if oper == 'Продажа' else -1
            a['quantity_sells_nm'] += qty * mult
            a['retail_price_nm'] += price * mult
            a['retail_amount_nm'] += amount * mult
            a['ppvz_for_pay_nm'] += ppvz * mult
            a['rub_commission_nm'] += (amount * comm_prc / 100.0) if comm_prc else 0.0
            a['rub_spp_nm'] += (amount * spp_prc / 100.0) if spp_prc else 0.0
        if oper == 'Возврат':
            a['quantity_return_nm'] += qty
        if oper == 'Логистика':
            a['delivery_amount_nm'] += delivery_amt_cnt
            a['return_amount_nm'] += return_amt_cnt
            a['delivery_rub_nm'] += delivery_rub

    for nm, a in agg.items():
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
    resp = supabase.table('week_rows').select('*').eq('realizationreport_id', realizationreport_id).execute()
    rows = []
    for r in (resp.data or []):
        nm = r.get('nm_id')
        if nm is None or int(nm) <= 0:
            continue
        if r.get('product_id') is None:
            continue
        rows.append(r)
    return rows


def insert_week_stats_for_report(
    supabase: Client,
    realizationreport_id: int
) -> Tuple[int, int]:
    date_from, date_to, report_type = get_week_report_dates_and_type(supabase, realizationreport_id)
    products_map = get_products_map(supabase)

    db_rows = load_rows_from_db(supabase, realizationreport_id)
    agg = aggregate_week_rows_by_nm(db_rows)

    candidates: List[Dict[str, Any]] = []
    for nm_id, a in agg.items():
        product_id = products_map.get(nm_id)
        if product_id is None:
            continue
        rec = {
            'report_type': report_type,
            'realizationreport_id': realizationreport_id,
            'product_id': product_id,
            'date_from': date_from,
            'date_to': date_to,
            'nm_id': nm_id,
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

    existing_resp = supabase.table('week_stats').select('realizationreport_id, nm_id').eq('realizationreport_id', realizationreport_id).execute()
    existing_pairs = {(row['realizationreport_id'], row['nm_id']) for row in (existing_resp.data or [])}

    to_insert = [rec for rec in candidates if (rec['realizationreport_id'], rec['nm_id']) not in existing_pairs]

    total_unique_nm = len(agg.keys())
    will_insert = len(to_insert)

    if not to_insert:
        return 0, total_unique_nm

    resp = supabase.table('week_stats').insert(to_insert).execute()
    inserted = len(resp.data) if resp.data else len(to_insert)
    return inserted, total_unique_nm


def verify_week_stats_totals(
    supabase: Client,
    realizationreport_id: int
) -> None:
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
