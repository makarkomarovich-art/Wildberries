"""
Функции агрегации данных из API reportDetailByPeriod.
"""

from typing import Dict, List, Any


def aggregate_by_oper_name(records_by_report: Dict[int, List[Dict[str, Any]]]) -> Dict[int, Dict[str, Any]]:
    """
    Агрегирует данные API по realizationreport_id с учетом логики по supplier_oper_name.
    
    Args:
        records_by_report: Dict с ключами realizationreport_id и списками записей
        
    Returns:
        Dict с ключами realizationreport_id и агрегированными значениями
    """
    aggregated = {}
    
    for report_id, records in records_by_report.items():
        aggregated[report_id] = {
            'realizationreport_id': report_id,
            'report_type': records[0].get('report_type') if records else None,
            'date_from': records[0].get('date_from') if records else None,
            'date_to': records[0].get('date_to') if records else None,
            'create_dt': records[0].get('create_dt') if records else None,
            
            # Начальные значения для агрегации
            'quantity_sells_total': 0,
            'quantity_return_total': 0,
            'retail_price_total': 0,
            'retail_amount': 0,
            'ppvz_for_pay': 0,
            'delivery_amount_total': 0,
            'return_amount_total': 0,
            'delivery_rub': 0,
            'penalty': 0,
            'storage_fee': 0,
            'deduction': 0,
            'acceptance': 0,
            'rows_count': 0
        }
        
        for record in records:
            oper_name = record.get('supplier_oper_name', '')
            
            # Продажа и Возврат: retail_amount, ppvz_for_pay, quantity, retail_price
            if oper_name in ['Продажа', 'Возврат']:
                multiplier = 1 if oper_name == 'Продажа' else -1
                aggregated[report_id]['retail_amount'] += (record.get('retail_amount', 0) or 0) * multiplier
                aggregated[report_id]['ppvz_for_pay'] += (record.get('ppvz_for_pay', 0) or 0) * multiplier
                aggregated[report_id]['quantity_sells_total'] += (record.get('quantity', 0) or 0) * multiplier
                aggregated[report_id]['retail_price_total'] += (record.get('retail_price', 0) or 0) * multiplier
            
            # Возврат: quantity для подсчета возвратов
            if oper_name == 'Возврат':
                aggregated[report_id]['quantity_return_total'] += record.get('quantity', 0) or 0
            
            # Логистика: delivery_rub, delivery_amount, return_amount
            if oper_name == 'Логистика':
                aggregated[report_id]['delivery_rub'] += record.get('delivery_rub', 0) or 0
                aggregated[report_id]['delivery_amount_total'] += record.get('delivery_amount', 0) or 0
                aggregated[report_id]['return_amount_total'] += record.get('return_amount', 0) or 0
            
            # Хранение: storage_fee
            if oper_name == 'Хранение':
                aggregated[report_id]['storage_fee'] += record.get('storage_fee', 0) or 0
            
            # Штраф: penalty
            if oper_name == 'Штраф':
                aggregated[report_id]['penalty'] += record.get('penalty', 0) or 0
            
            # Платная приемка: acceptance
            if oper_name == 'Платная приемка':
                aggregated[report_id]['acceptance'] += record.get('acceptance', 0) or 0
            
            # Удержание: deduction
            if oper_name == 'Удержание':
                aggregated[report_id]['deduction'] += record.get('deduction', 0) or 0
            
            aggregated[report_id]['rows_count'] += 1
        
        # Вычисляем cancels_total
        aggregated[report_id]['cancels_total'] = (
            aggregated[report_id]['return_amount_total'] - 
            aggregated[report_id]['quantity_return_total']
        )
        
        # Вычисляем rub_discountWB_both_total
        aggregated[report_id]['rub_discountWB_both_total'] = (
            aggregated[report_id]['retail_price_total'] - 
            aggregated[report_id]['retail_amount']
        )
        
        # Вычисляем perc_discountWB_both_total
        if aggregated[report_id]['retail_price_total'] != 0:
            aggregated[report_id]['perc_discountWB_both_total'] = (
                aggregated[report_id]['rub_discountWB_both_total'] / 
                aggregated[report_id]['retail_price_total']
            ) * 100
        else:
            aggregated[report_id]['perc_discountWB_both_total'] = 0
        
        # Вычисляем rub_commision_both_total
        aggregated[report_id]['rub_commision_both_total'] = (
            aggregated[report_id]['retail_price_total'] - 
            aggregated[report_id]['ppvz_for_pay']
        )
        
        # Вычисляем perc_commisian_both_total
        if aggregated[report_id]['retail_price_total'] != 0:
            aggregated[report_id]['perc_commisian_both_total'] = (
                aggregated[report_id]['rub_commision_both_total'] / 
                aggregated[report_id]['retail_price_total']
            ) * 100
        else:
            aggregated[report_id]['perc_commisian_both_total'] = 0
        
        # Вычисляем bankPaymentSum
        aggregated[report_id]['bankPaymentSum'] = (
            aggregated[report_id]['ppvz_for_pay'] - 
            aggregated[report_id]['delivery_rub'] - 
            aggregated[report_id]['storage_fee'] - 
            aggregated[report_id]['penalty'] - 
            aggregated[report_id]['acceptance'] - 
            aggregated[report_id]['deduction']
        )
    
    return aggregated
