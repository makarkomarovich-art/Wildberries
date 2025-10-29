"""
Функции сравнения значений API и CURL.
"""

from typing import Dict, Any, Tuple


def compare_api_curl(
    api_aggregated: Dict[str, Any],
    curl_data: Dict[str, Any],
    tolerance: float = 0.01
) -> Tuple[bool, Dict[str, Any]]:
    """
    Валидирует данные, сравнивая API агрегат с CURL данными.
    
    Args:
        api_aggregated: Агрегированные данные из API
        curl_data: Данные из CURL API
        tolerance: Допустимая погрешность (по умолчанию 1 копейка)
        
    Returns:
        Tuple[bool, Dict]: (успех валидации, детали сравнения)
    """
    results = {
        'valid': True,
        'fields': {}
    }
    
    # Сравниваем поля
    comparisons = {
        'retail_amount': curl_data.get('totalSale'),
        'ppvz_for_pay': curl_data.get('forPay'),
        'delivery_rub': curl_data.get('deliveryRub'),
        'penalty': curl_data.get('penalty'),
        'storage_fee': curl_data.get('paidStorageSum'),
        'acceptance': curl_data.get('paidAcceptanceSum'),
        'deduction': curl_data.get('paidWithholdingSum'),
        'bankPaymentSum': curl_data.get('bankPaymentSum')
    }
    
    for api_field, curl_value in comparisons.items():
        api_value = api_aggregated.get(api_field, 0)
        
        if curl_value is None:
            diff = 0
            valid = False
        else:
            diff = abs(api_value - curl_value)
            valid = diff <= tolerance
        
        results['fields'][api_field] = {
            'api_value': api_value,
            'curl_value': curl_value,
            'diff': diff,
            'valid': valid
        }
        
        if not valid:
            results['valid'] = False
    
    return results['valid'], results
