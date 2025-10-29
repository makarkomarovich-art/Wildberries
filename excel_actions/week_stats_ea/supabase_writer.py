"""
Функции для записи данных weekly reports в Supabase.
"""

from __future__ import annotations

from typing import Dict, Any, List
from supabase import Client


def upsert_week_reports(
    aggregated_data: Dict[int, Dict[str, Any]],
    supabase: Client
) -> int:
    """
    Upsert агрегированных данных в таблицу week_reports.
    
    Args:
        aggregated_data: Dict с ключами realizationreport_id и агрегированными значениями
        supabase: Клиент Supabase
    
    Returns:
        Количество обработанных записей
    
    Raises:
        Exception: При ошибке записи в БД
    """
    if not aggregated_data:
        print("⚠️  Нет записей для upsert")
        return 0
    
    print(f"🔄 Upsert в week_reports...")
    print(f"   Записей: {len(aggregated_data)}")
    
    # Конвертируем данные в формат для БД
    records = []
    for report_id, data in aggregated_data.items():
        record = {
            'realizationreport_id': report_id,
            'report_type': data.get('report_type'),
            'date_from': data.get('date_from'),
            'date_to': data.get('date_to'),
            
            # Количество
            'quantity_sells_total': data.get('quantity_sells_total'),
            'quantity_return_total': data.get('quantity_return_total'),
            'cancels_total': data.get('cancels_total'),
            
            # Цены
            'retail_price_total': data.get('retail_price_total'),
            'retail_amount_total': data.get('retail_amount'),
            'rub_discountwb_both_total': data.get('rub_discountWB_both_total'),
            'perc_discountwb_both_total': data.get('perc_discountWB_both_total'),
            
            # Выплаты
            'ppvz_for_pay_total': data.get('ppvz_for_pay'),
            'rub_commision_both_total': data.get('rub_commision_both_total'),
            'perc_commisian_both_total': data.get('perc_commisian_both_total'),
            
            # Логистика
            'delivery_amount_total': data.get('delivery_amount_total'),
            'return_amount_total': data.get('return_amount_total'),
            'delivery_rub_total': data.get('delivery_rub'),
            
            # Доп. услуги
            'penalty_total': data.get('penalty'),
            'storage_fee_total': data.get('storage_fee'),
            'deduction_total': data.get('deduction'),
            'acceptance_total': data.get('acceptance'),
        }
        records.append(record)
    
    try:
        # Проверяем, какие отчеты уже есть в БД
        existing_ids = []
        for record in records:
            response = supabase.table('week_reports').select('realizationreport_id').eq('realizationreport_id', record['realizationreport_id']).execute()
            if response.data:
                existing_ids.append(record['realizationreport_id'])
        
        # Фильтруем только новые записи
        new_records = [r for r in records if r['realizationreport_id'] not in existing_ids]
        
        if not new_records:
            print("⚠️  Все отчеты уже существуют в БД")
            return 0
        
        # Вставляем только новые записи
        response = supabase.table('week_reports').insert(new_records).execute()
        
        count = len(response.data) if response.data else len(new_records)
        print(f"✅ Обработано записей в week_reports: {count} (новых: {len(new_records)}, существующих: {len(existing_ids)})")
        
        return count
    
    except Exception as e:
        print(f"❌ ОШИБКА при insert в week_reports: {e}")
        raise
