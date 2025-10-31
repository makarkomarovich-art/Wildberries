"""
Тесты для Supplies to Warehouses (поставки товаров на склады WB).

Тестируем:
1. Валидацию структуры ответа API /api/v1/supplier/incomes
2. Валидацию структуры curl ответа supplyDetails
3. Трансформацию данных и fallback логику
4. Обработку edge cases (пустые данные, отсутствующие поля)
"""
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Добавляем корень проекта в sys.path для импортов
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from excel_actions.supplies_to_warehouses_ea.structure_validator import (
    validate_incomes_structure,
    validate_incomes_with_schema
)
from excel_actions.supplies_to_warehouses_ea.supply_details_validator import (
    validate_supply_details_structure,
    extract_delivery_expr
)
from excel_actions.supplies_to_warehouses_ea.transform import (
    filter_accepted_supplies,
    prepare_for_db,
    convert_delivery_expr_to_numeric,
    add_delivery_expr_to_records,
    apply_fallback_delivery_expr
)
from excel_actions.supplies_to_warehouses_ea.supabase_writer import (
    enrich_with_product_ids,
    find_fallback_delivery_expr
)


class TestSuppliesIncomesValidation:
    """Тесты для валидации структуры incomes API"""
    
    def test_valid_incomes_passes_validation(self, load_fixture):
        """
        Тест #1: Валидный ответ от incomes API должен проходить валидацию.
        
        Проверяем:
        - Все обязательные поля присутствуют
        - Типы данных корректны
        - Валидатор возвращает True
        """
        # Загружаем валидный ответ
        data = load_fixture('supplies_to_warehouses/incomes_valid.json')
        
        # Проверяем что есть поставки
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Валидация должна пройти
        result = validate_incomes_structure(data)
        assert result == True, "Валидный ответ должен проходить валидацию"
    
    def test_empty_incomes_passes_with_warning(self, load_fixture):
        """
        Тест #2: Пустой массив поставок должен проходить валидацию с предупреждением.
        
        Проверяем:
        - API вернул корректную структуру, но без поставок
        - Валидатор возвращает True (это не ошибка, просто нет данных)
        """
        # Загружаем ответ с пустым массивом
        data = load_fixture('supplies_to_warehouses/incomes_empty.json')
        
        # Проверяем структуру
        assert isinstance(data, list)
        assert len(data) == 0
        
        # Валидация должна пройти (это не ошибка)
        result = validate_incomes_structure(data)
        assert result == True, "Пустой массив поставок не является ошибкой"
    
    def test_missing_incomeId_fails_validation(self, load_fixture):
        """
        Тест #3: Отсутствие incomeId в поставке должно провалить валидацию.
        
        Проверяем:
        - incomeId - критичное поле (без него нельзя идентифицировать поставку)
        - Валидатор возвращает False
        """
        # Загружаем ответ без incomeId
        data = load_fixture('supplies_to_warehouses/incomes_missing_incomeId.json')
        
        # Проверяем что структура есть, но incomeId отсутствует
        assert isinstance(data, list)
        assert len(data) > 0
        
        first_income = data[0]
        assert 'incomeId' not in first_income, "В фикстуре не должно быть incomeId"
        
        # Валидация должна провалиться
        result = validate_incomes_structure(data)
        assert result == False, "Отсутствие incomeId должно провалить валидацию"
    
    def test_missing_nmId_fails_validation(self, load_fixture):
        """
        Тест #4: Отсутствие nmId должно провалить валидацию.
        
        Проверяем:
        - nmId - обязательное поле
        - Валидатор возвращает False
        """
        data = load_fixture('supplies_to_warehouses/incomes_missing_nmId.json')
        
        first_income = data[0]
        assert 'nmId' not in first_income
        
        result = validate_incomes_structure(data)
        assert result == False, "Отсутствие nmId должно провалить валидацию"
    
    def test_missing_status_fails_validation(self, load_fixture):
        """
        Тест #5: Отсутствие status должно провалить валидацию.
        
        Проверяем:
        - status - обязательное поле
        - Валидатор возвращает False
        """
        data = load_fixture('supplies_to_warehouses/incomes_missing_status.json')
        
        first_income = data[0]
        assert 'status' not in first_income
        
        result = validate_incomes_structure(data)
        assert result == False, "Отсутствие status должно провалить валидацию"
    
    def test_wrong_type_incomeId_fails_validation(self, load_fixture):
        """
        Тест #6: Неправильный тип incomeId (string вместо integer) должен провалить валидацию.
        
        Проверяем:
        - incomeId должен быть integer
        - Если string - валидация должна провалиться
        """
        data = load_fixture('supplies_to_warehouses/incomes_wrong_type_incomeId.json')
        
        first_income = data[0]
        assert isinstance(first_income['incomeId'], str), "incomeId должен быть строкой в фикстуре"
        
        result = validate_incomes_structure(data)
        assert result == False, "incomeId типа string должен провалить валидацию"
    
    def test_filter_accepted_supplies(self, load_fixture):
        """
        Тест #7: Фильтрация поставок по статусу "Принято".
        
        Проверяем:
        - Фильтруются только поставки со статусом "Принято"
        - Подсчитывается количество пропущенных
        """
        data = load_fixture('supplies_to_warehouses/incomes_mixed_status.json')
        
        # В фикстуре должно быть 2 поставки: одна "Принято", одна "В обработке"
        assert len(data) == 2
        
        accepted = filter_accepted_supplies(data)
        assert len(accepted) == 1, "Должна быть отфильтрована 1 поставка со статусом 'Принято'"
        assert accepted[0]['status'] == "Принято"


class TestSuppliesDetailsValidation:
    """Тесты для валидации структуры supply details API"""
    
    def test_valid_supply_details_passes_validation(self, load_fixture):
        """
        Тест #1: Валидный ответ от supply details API должен проходить валидацию.
        
        Проверяем:
        - Все обязательные поля присутствуют
        - Типы данных корректны
        - Валидатор возвращает True
        """
        # Загружаем валидный ответ
        data = load_fixture('supplies_to_warehouses/supply_details_valid.json')
        
        # Проверяем что есть result
        assert isinstance(data, dict)
        assert 'result' in data
        
        # Валидация должна пройти
        result = validate_supply_details_structure(data)
        assert result == True, "Валидный supply details ответ должен проходить валидацию"
    
    def test_missing_deliveryAndStorageExpr_fails_validation(self, load_fixture):
        """
        Тест #2: Отсутствие deliveryAndStorageExpr должно провалить валидацию.
        
        Проверяем:
        - deliveryAndStorageExpr - обязательное поле
        - Валидатор возвращает False
        """
        data = load_fixture('supplies_to_warehouses/supply_details_missing_delivery_expr.json')
        
        # Проверяем что структура есть, но deliveryAndStorageExpr отсутствует
        assert 'result' in data
        result_obj = data['result']
        assert 'supply' in result_obj
        supply_obj = result_obj['supply']
        assert 'deliveryAndStorage' in supply_obj
        delivery_obj = supply_obj['deliveryAndStorage']
        assert 'deliveryAndStorageExpr' not in delivery_obj
        
        # Валидация должна провалиться
        result = validate_supply_details_structure(data)
        assert result == False, "Отсутствие deliveryAndStorageExpr должно провалить валидацию"
    
    def test_extract_delivery_expr(self, load_fixture):
        """
        Тест #3: Извлечение deliveryAndStorageExpr из валидного ответа.
        
        Проверяем:
        - Корректное извлечение значения
        - Обработка null значений
        """
        # Валидный ответ
        valid_data = load_fixture('supplies_to_warehouses/supply_details_valid.json')
        delivery_expr = extract_delivery_expr(valid_data)
        assert delivery_expr is not None
        assert delivery_expr == "170"
        
        # Ответ без deliveryAndStorageExpr
        invalid_data = load_fixture('supplies_to_warehouses/supply_details_missing_delivery_expr.json')
        delivery_expr = extract_delivery_expr(invalid_data)
        assert delivery_expr is None
    
    def test_wrong_type_deliveryAndStorageExpr_passes_validation(self, load_fixture):
        """
        Тест #4: Неправильный тип deliveryAndStorageExpr (number вместо string) проходит валидацию.
        
        Проверяем:
        - Наш валидатор проверяет только наличие поля
        - Тип не критичен для нашей логики
        """
        data = load_fixture('supplies_to_warehouses/supply_details_wrong_type_delivery_expr.json')
        
        result_obj = data['result']['supply']['deliveryAndStorage']
        assert isinstance(result_obj['deliveryAndStorageExpr'], int), "deliveryAndStorageExpr должен быть числом в фикстуре"
        
        # Валидация должна пройти (мы проверяем только наличие)
        result = validate_supply_details_structure(data)
        assert result == True, "deliveryAndStorageExpr любого типа должен проходить валидацию"


class TestSuppliesTransform:
    """Тесты для трансформации данных"""
    
    def test_prepare_for_db(self, load_fixture):
        """
        Тест #1: Подготовка данных для БД.
        
        Проверяем:
        - Преобразование полей в формат БД
        - Конвертация дат в ISO формат
        - Корректность типов данных
        """
        data = load_fixture('supplies_to_warehouses/incomes_valid.json')
        accepted = filter_accepted_supplies(data)
        
        records = prepare_for_db(accepted)
        
        assert len(records) == len(accepted)
        
        # Проверяем преобразование полей
        record = records[0]
        assert 'income_id' in record
        assert 'nm_id' in record
        assert 'supplier_article' in record
        assert 'warehouse_name' in record
        assert 'date' in record
        assert 'last_change_date' in record
        
        # Проверяем типы
        assert isinstance(record['income_id'], int)
        assert isinstance(record['nm_id'], int)
        assert isinstance(record['quantity'], int)
        assert isinstance(record['date'], str)  # ISO формат
        assert isinstance(record['last_change_date'], str)  # ISO формат
    
    def test_convert_delivery_expr_to_numeric(self):
        """
        Тест #2: Конвертация delivery_expr в числовой формат.
        
        Проверяем:
        - Конвертация строки в Decimal
        - Обработка различных форматов
        - Обработка null значений
        """
        from decimal import Decimal
        
        # Валидные значения
        assert convert_delivery_expr_to_numeric("170") == Decimal("170")
        assert convert_delivery_expr_to_numeric("160.5") == Decimal("160.5")
        assert convert_delivery_expr_to_numeric("0") == Decimal("0")
        
        # Null значения
        assert convert_delivery_expr_to_numeric(None) is None
        assert convert_delivery_expr_to_numeric("") is None
        
        # Невалидные значения
        assert convert_delivery_expr_to_numeric("invalid") is None
        assert convert_delivery_expr_to_numeric("abc123") is None
    
    def test_add_delivery_expr_to_records(self):
        """
        Тест #3: Добавление delivery_expr к записям.
        
        Проверяем:
        - Корректное добавление значений
        - Обработка отсутствующих значений
        """
        records = [
            {"income_id": 123, "nm_id": 456},
            {"income_id": 789, "nm_id": 101}
        ]
        
        delivery_data = {
            123: "170",
            789: None
        }
        
        result = add_delivery_expr_to_records(records, delivery_data)
        
        assert len(result) == 2
        assert result[0]["delivery_and_storage_expr"] == "170"
        assert result[1]["delivery_and_storage_expr"] is None
    
    def test_apply_fallback_delivery_expr(self):
        """
        Тест #4: Применение fallback логики.
        
        Проверяем:
        - Fallback применяется только к записям без delivery_expr
        - Записи с delivery_expr остаются без изменений
        """
        records = [
            {"income_id": 123, "nm_id": 456, "delivery_and_storage_expr": "170"},
            {"income_id": 789, "nm_id": 101, "delivery_and_storage_expr": None}
        ]
        
        # Мокаем supabase клиент
        mock_supabase = Mock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.not_.return_value.lt.return_value.order.return_value.limit.return_value.execute.return_value.data = [
            {"delivery_and_storage_expr": "160", "date": "2025-10-20T00:00:00+00:00"}
        ]
        
        result = apply_fallback_delivery_expr(records, mock_supabase)
        
        assert len(result) == 2
        # Первая запись не изменилась
        assert result[0]["delivery_and_storage_expr"] == "170"
        # Вторая запись получила fallback
        assert result[1]["delivery_and_storage_expr"] == 160.0


class TestSuppliesSupabaseWriter:
    """Тесты для записи в Supabase"""
    
    def test_enrich_with_product_ids(self):
        """
        Тест #1: Обогащение записей product_id.
        
        Проверяем:
        - Корректное получение product_id из БД
        - Фильтрация записей без product_id
        """
        records = [
            {"nm_id": 456, "income_id": 123},
            {"nm_id": 999, "income_id": 789}  # Несуществующий nm_id
        ]
        
        # Мокаем supabase клиент
        mock_supabase = Mock()
        mock_supabase.table.return_value.select.return_value.execute.return_value.data = [
            {"nm_id": 456, "id": "product-uuid-123"}
        ]
        
        result = enrich_with_product_ids(records, mock_supabase)
        
        # Должна остаться только одна запись (с существующим nm_id)
        assert len(result) == 1
        assert result[0]["product_id"] == "product-uuid-123"
        assert result[0]["nm_id"] == 456
    
    def test_find_fallback_delivery_expr(self):
        """
        Тест #2: Поиск fallback delivery_expr.
        
        Проверяем:
        - Поиск предыдущих поставок
        - Возврат корректного значения
        """
        # Мокаем supabase клиент
        mock_supabase = Mock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.not_.return_value.lt.return_value.order.return_value.limit.return_value.execute.return_value.data = [
            {"delivery_and_storage_expr": "160", "date": "2025-10-20T00:00:00+00:00"}
        ]
        
        result = find_fallback_delivery_expr(
            mock_supabase, 
            "Электросталь", 
            456, 
            "2025-10-25T00:00:00"
        )
        
        assert result == 160.0
    
    def test_find_fallback_delivery_expr_not_found(self):
        """
        Тест #3: Fallback не найден.
        
        Проверяем:
        - Возврат None при отсутствии предыдущих поставок
        """
        # Мокаем supabase клиент
        mock_supabase = Mock()
        mock_supabase.table.return_value.select.return_value.eq.return_value.eq.return_value.not_.return_value.lt.return_value.order.return_value.limit.return_value.execute.return_value.data = []
        
        result = find_fallback_delivery_expr(
            mock_supabase, 
            "Электросталь", 
            456, 
            "2025-10-25T00:00:00"
        )
        
        assert result is None
