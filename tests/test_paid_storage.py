"""
Тесты для Paid Storage модуля.

Тестируем:
1. Валидацию структуры ответа API
2. Агрегацию данных по (date, nm_id)
3. Writer (обогащение product_id, фильтрация, upsert)
"""
import sys
from pathlib import Path
from decimal import Decimal
from unittest.mock import MagicMock, patch
import pytest

# Добавляем корень проекта в sys.path для импортов
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from excel_actions.paid_storage_ea.structure_validator import (
    validate_paid_storage_response,
    get_validation_report,
    ValidationError,
)
from excel_actions.paid_storage_ea.transform import (
    aggregate_paid_storage,
    summarize_by_day,
)
from excel_actions.paid_storage_ea.supabase_writer import (
    build_products_map,
    split_known_unknown_nmids,
    upsert_paid_storage,
)


class TestPaidStorageValidation:
    """Тесты для валидации структуры Paid Storage API"""
    
    def test_valid_response_passes(self, load_fixture):
        """Валидный ответ должен проходить валидацию"""
        data = load_fixture('paid_storage/paid_storage_valid.json')
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Валидация должна пройти без исключения
        validate_paid_storage_response(data)
    
    def test_empty_array_passes(self, load_fixture):
        """Пустой массив - валидный случай"""
        data = load_fixture('paid_storage/paid_storage_empty.json')
        
        assert data == []
        
        # Валидация должна пройти
        validate_paid_storage_response(data)
    
    def test_not_array_fails(self, load_fixture):
        """Ответ не является массивом - должна быть ошибка"""
        data = load_fixture('paid_storage/paid_storage_not_array.json')
        
        assert not isinstance(data, list)
        
        with pytest.raises(ValidationError) as exc_info:
            validate_paid_storage_response(data)
        
        assert "array" in str(exc_info.value).lower()
    
    def test_missing_date_fails(self, load_fixture):
        """Отсутствие поля date - должна быть ошибка"""
        data = load_fixture('paid_storage/paid_storage_missing_date.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_paid_storage_response(data)
        
        assert "date" in str(exc_info.value).lower()
    
    def test_missing_nmId_fails(self, load_fixture):
        """Отсутствие поля nmId - должна быть ошибка"""
        data = load_fixture('paid_storage/paid_storage_missing_nmId.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_paid_storage_response(data)
        
        assert "nmId" in str(exc_info.value).lower() or "nmid" in str(exc_info.value).lower()
    
    def test_missing_vendorCode_fails(self, load_fixture):
        """Отсутствие поля vendorCode - должна быть ошибка"""
        data = load_fixture('paid_storage/paid_storage_missing_vendorCode.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_paid_storage_response(data)
        
        assert "vendorcode" in str(exc_info.value).lower()
    
    def test_missing_warehousePrice_fails(self, load_fixture):
        """Отсутствие поля warehousePrice - должна быть ошибка"""
        data = load_fixture('paid_storage/paid_storage_missing_warehousePrice.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_paid_storage_response(data)
        
        assert "warehouseprice" in str(exc_info.value).lower()
    
    def test_wrong_type_nmId_fails(self, load_fixture):
        """nmId должен быть integer"""
        data = load_fixture('paid_storage/paid_storage_wrong_type_nmId.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_paid_storage_response(data)
        
        assert "nmid" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower()
    
    def test_get_validation_report_valid(self, load_fixture):
        """Отчёт валидации для валидных данных"""
        data = load_fixture('paid_storage/paid_storage_valid.json')
        
        report = get_validation_report(data)
        
        assert report['valid'] == True
        assert report['errors'] == []
        assert report['error_count'] == 0
    
    def test_get_validation_report_invalid(self, load_fixture):
        """Отчёт валидации для некорректных данных"""
        data = load_fixture('paid_storage/paid_storage_missing_date.json')
        
        report = get_validation_report(data)
        
        assert report['valid'] == False
        assert len(report['errors']) > 0
        assert report['error_count'] > 0


class TestPaidStorageTransform:
    """Тесты для агрегации данных"""
    
    def test_aggregate_by_date_and_nm_id(self, load_fixture):
        """Агрегация по (date, nm_id) с суммированием warehousePrice"""
        data = load_fixture('paid_storage/paid_storage_valid.json')
        
        result = aggregate_paid_storage(data)
        
        # Должно быть 3 записи: (2025-10-28, 123456), (2025-10-28, 789012), (2025-10-29, 123456)
        assert len(result) == 3
        
        # Проверяем первую запись: 2025-10-28, 123456 (10.5 + 5.3 = 15.8)
        r1 = next(r for r in result if r['date'] == '2025-10-28' and r['nm_id'] == 123456)
        assert r1['warehouse_price'] == Decimal('15.8')
        assert set(r1['gi_ids']) == {100, 101}
        
        # Проверяем вторую запись: 2025-10-28, 789012
        r2 = next(r for r in result if r['date'] == '2025-10-28' and r['nm_id'] == 789012)
        assert r2['warehouse_price'] == Decimal('20.0')
        assert r2['gi_ids'] == [200]
        
        # Проверяем третью запись: 2025-10-29, 123456
        r3 = next(r for r in result if r['date'] == '2025-10-29' and r['nm_id'] == 123456)
        assert r3['warehouse_price'] == Decimal('15.0')
        assert r3['gi_ids'] == [102]
    
    def test_aggregate_rounding(self):
        """Проверка округления до 2 знаков"""
        data = [
            {"date": "2025-10-28", "nmId": 123, "vendorCode": "test", "warehousePrice": 1.2345}
        ]
        
        result = aggregate_paid_storage(data)
        
        assert len(result) == 1
        assert result[0]['warehouse_price'] == Decimal('1.23')
    
    def test_aggregate_empty_gi_ids(self):
        """Если giId отсутствует, gi_ids должен быть None"""
        data = [
            {"date": "2025-10-28", "nmId": 123, "vendorCode": "test", "warehousePrice": 10.0}
        ]
        
        result = aggregate_paid_storage(data)
        
        assert len(result) == 1
        assert result[0]['gi_ids'] is None
    
    def test_aggregate_deduplicate_gi_ids(self):
        """gi_ids должны дедуплицироваться и сортироваться"""
        data = [
            {"date": "2025-10-28", "nmId": 123, "vendorCode": "test", "warehousePrice": 10.0, "giId": 200},
            {"date": "2025-10-28", "nmId": 123, "vendorCode": "test", "warehousePrice": 5.0, "giId": 100},
            {"date": "2025-10-28", "nmId": 123, "vendorCode": "test", "warehousePrice": 3.0, "giId": 200},  # дубликат
        ]
        
        result = aggregate_paid_storage(data)
        
        assert len(result) == 1
        assert result[0]['gi_ids'] == [100, 200]  # отсортировано и без дублей
    
    def test_summarize_by_day(self, load_fixture):
        """Сводка по дням"""
        data = load_fixture('paid_storage/paid_storage_valid.json')
        aggregated = aggregate_paid_storage(data)
        
        summary = summarize_by_day(aggregated)
        
        # Должно быть 2 дня
        assert len(summary) == 2
        
        # 2025-10-28: 2 уникальных nm_id, total = 35.8
        s1 = next(s for s in summary if s['date'] == '2025-10-28')
        assert s1['unique_nm_count'] == 2
        assert s1['total_warehouse_price'] == Decimal('35.8')
        
        # 2025-10-29: 1 уникальный nm_id, total = 15.0
        s2 = next(s for s in summary if s['date'] == '2025-10-29')
        assert s2['unique_nm_count'] == 1
        assert s2['total_warehouse_price'] == Decimal('15.0')


class TestPaidStorageWriter:
    """Тесты для записи в Supabase"""
    
    def test_build_products_map(self):
        """Создание маппинга nm_id -> product_id"""
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.execute.return_value.data = [
            {'nm_id': 123456, 'id': 'uuid-123'},
            {'nm_id': 789012, 'id': 'uuid-789'},
        ]
        
        result = build_products_map(mock_supabase)
        
        assert result == {123456: 'uuid-123', 789012: 'uuid-789'}
        mock_supabase.table.assert_called_once_with('products')
    
    def test_split_known_unknown_nmids(self):
        """Разделение на известные и неизвестные nm_id"""
        aggregated = [
            {'nm_id': 123456, 'date': '2025-10-28', 'vendor_code': 'test1', 'warehouse_price': 10.0, 'gi_ids': None},
            {'nm_id': 789012, 'date': '2025-10-28', 'vendor_code': 'test2', 'warehouse_price': 20.0, 'gi_ids': None},
            {'nm_id': 999999, 'date': '2025-10-28', 'vendor_code': 'test3', 'warehouse_price': 30.0, 'gi_ids': None},
        ]
        products_map = {123456: 'uuid-123', 789012: 'uuid-789'}
        
        known, missing = split_known_unknown_nmids(aggregated, products_map)
        
        assert len(known) == 2
        assert len(missing) == 1
        assert 999999 in missing
        
        # Проверяем обогащение product_id
        assert known[0]['product_id'] == 'uuid-123'
        assert known[1]['product_id'] == 'uuid-789'
        
        # Проверяем что Decimal конвертирован в float
        assert isinstance(known[0]['warehouse_price'], float)
    
    def test_upsert_paid_storage_new_records(self):
        """Upsert новых записей"""
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.gte.return_value.lte.return_value.execute.return_value.data = []
        
        rows = [
            {'nm_id': 123456, 'date': '2025-10-28', 'vendor_code': 'test1', 'warehouse_price': 10.0, 'product_id': 'uuid-123', 'gi_ids': None},
        ]
        
        mock_supabase.table.return_value.upsert.return_value.execute.return_value.data = rows
        
        processed, updated = upsert_paid_storage(rows, mock_supabase)
        
        assert processed == 1
        assert len(updated) == 0  # новые записи, не обновления
        mock_supabase.table.return_value.upsert.assert_called_once()
    
    def test_upsert_paid_storage_updated_records(self):
        """Upsert с обновлением изменённых записей"""
        mock_supabase = MagicMock()
        # Симулируем существующую запись
        mock_supabase.table.return_value.select.return_value.gte.return_value.lte.return_value.execute.return_value.data = [
            {'nm_id': 123456, 'date': '2025-10-28', 'warehouse_price': 5.0}
        ]
        
        rows = [
            {'nm_id': 123456, 'date': '2025-10-28', 'vendor_code': 'test1', 'warehouse_price': 10.0, 'product_id': 'uuid-123', 'gi_ids': None},
        ]
        
        mock_supabase.table.return_value.upsert.return_value.execute.return_value.data = rows
        
        processed, updated = upsert_paid_storage(rows, mock_supabase)
        
        assert processed == 1
        assert len(updated) == 1  # одна запись обновлена
        assert updated[0]['nm_id'] == 123456
        assert updated[0]['old_price'] == 5.0
        assert updated[0]['new_price'] == 10.0
    
    def test_upsert_paid_storage_no_changes(self):
        """Upsert без изменений - не обновляет"""
        mock_supabase = MagicMock()
        # Симулируем существующую запись с той же ценой
        # Нужно правильно настроить цепочку вызовов для Supabase client
        mock_select_chain = MagicMock()
        mock_select_chain.execute.return_value.data = [
            {'nm_id': 123456, 'date': '2025-10-28', 'warehouse_price': 10.0}
        ]
        mock_gte_chain = MagicMock()
        mock_gte_chain.lte.return_value = mock_select_chain
        mock_select_chain.gte.return_value = mock_gte_chain
        mock_table_select = MagicMock()
        mock_table_select.select.return_value = mock_select_chain
        mock_table = MagicMock()
        mock_table.return_value = mock_table_select
        mock_supabase.table = mock_table
        
        rows = [
            {'nm_id': 123456, 'date': '2025-10-28', 'vendor_code': 'test1', 'warehouse_price': 10.0, 'product_id': 'uuid-123', 'gi_ids': None},
        ]
        
        processed, updated = upsert_paid_storage(rows, mock_supabase)
        
        # Если цена не изменилась, to_write будет пустым, обработано 0
        assert processed == 0
        assert len(updated) == 0
    
    def test_upsert_paid_storage_empty_list(self):
        """Upsert пустого списка"""
        mock_supabase = MagicMock()
        
        processed, updated = upsert_paid_storage([], mock_supabase)
        
        assert processed == 0
        assert len(updated) == 0
        mock_supabase.table.assert_not_called()

