"""
Пост-валидация данных CR Daily Stats после записи в Supabase.

Проверяет что данные из API корректно записались в БД,
сравнивая ожидаемые значения с фактическими.
"""

from __future__ import annotations
from typing import List
from supabase import Client


def validate_inserted_data(
    expected_records: list[dict],
    date: str,
    supabase: Client,
    label: str = ""
) -> bool:
    """
    Проверяет что данные из API корректно записались в БД.
    
    Сравнивает все бизнес-поля из expected_records с данными из БД
    за указанную дату.
    
    Args:
        expected_records: Записи которые ожидали записать
        date: Дата для проверки в формате 'YYYY-MM-DD'
        supabase: Клиент Supabase
        label: Метка для логирования (например, "сегодня" или "вчера")
    
    Returns:
        True если все ОК, False если есть расхождения
    """
    if not expected_records:
        print(f"⚠️  Нет данных для валидации ({label})")
        return True
    
    # 1. Собрать nm_id которые ожидаем проверить
    expected_nm_ids = [r['nm_id'] for r in expected_records]
    
    # 2. Запросить данные из БД только для этих nm_id
    try:
        db_response = supabase.table('cr_daily_stats')\
            .select('*')\
            .eq('date_of_period', date)\
            .in_('nm_id', expected_nm_ids)\
            .execute()
        db_records = db_response.data
    except Exception as e:
        print(f"⚠️  WARNING: Ошибка при запросе данных из БД ({label}): {e}")
        return False
    
    # 3. Проверить количество записей
    expected_count = len(expected_records)
    actual_count = len(db_records)
    
    if expected_count != actual_count:
        print(f"⚠️  WARNING: Расхождение количества ({label}): "
              f"ожидали {expected_count}, в БД {actual_count}")
        return False
    
    # 4. Создать мапу БД записей по nm_id
    db_map = {r['nm_id']: r for r in db_records}
    
    # 5. Поля для проверки (все бизнес-поля из record)
    base_fields = [
        'nm_id',
        'vendor_code',
        'date_of_period',
        'open_card_count',
        'add_to_cart_count',
        'orders_count',
        'cancel_count',
        'orders_sum_rub',
        'add_to_cart_percent',
        'cart_to_order_percent',
        'order_price',
    ]
    
    # 6. Проверить каждую запись
    mismatches = []
    
    for expected in expected_records:
        nm_id = expected.get('nm_id')
        
        if nm_id not in db_map:
            mismatches.append(f"nm_id={nm_id} отсутствует в БД")
            continue
        
        actual = db_map[nm_id]
        
        # Определяем поля для проверки (для сегодняшних + stocks)
        fields_to_check = base_fields.copy()
        if 'stocks_mp' in expected:
            fields_to_check.extend(['stocks_mp', 'stocks_wb'])
        
        # Сравниваем каждое поле
        for field in fields_to_check:
            expected_value = expected.get(field)
            actual_value = actual.get(field)
            
            # Сравнение с учетом типов (None, int, float, str)
            if not _values_equal(expected_value, actual_value):
                mismatches.append(
                    f"nm_id={nm_id}, поле '{field}': "
                    f"ожидали {expected_value}, в БД {actual_value}"
                )
    
    # 7. Вывод результатов
    if mismatches:
        print(f"⚠️  Валидация ({label}): найдено {len(mismatches)} расхождений из {actual_count} записей")
        for m in mismatches[:10]:  # Показываем первые 10
            print(f"   ⚠️  WARNING: {m}")
        if len(mismatches) > 10:
            print(f"   ... и еще {len(mismatches) - 10} расхождений")
        return False
    
    print(f"✅ Валидация данных пройдена ({label}): {actual_count} записей")
    return True


def _values_equal(expected, actual) -> bool:
    """
    Сравнивает два значения с учетом типов и None.
    
    Args:
        expected: Ожидаемое значение
        actual: Фактическое значение
    
    Returns:
        True если значения равны
    """
    # Оба None - равны
    if expected is None and actual is None:
        return True
    
    # Одно None, другое нет - не равны
    if (expected is None) != (actual is None):
        return False
    
    # Для чисел - сравнение с точностью (могут быть float/Decimal различия)
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return abs(float(expected) - float(actual)) < 0.01
    
    # Для строк и дат - прямое сравнение
    return str(expected) == str(actual)

