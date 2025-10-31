"""
Тесты для Paid Acceptance модуля.

Тестируем:
1. Валидацию структуры ответа API
2. Агрегацию данных по (shkCreateDate, nmID)
3. Writer (обогащение product_id и vendor_code, фильтрация, upsert)
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

from excel_actions.paid_acceptance_ea.structure_validator import (
    validate_acceptance_response,
    get_validation_report,
    ValidationError,
)
from excel_actions.paid_acceptance_ea.transform import (
    aggregate_acceptance,
    per_article_logs,
)
from excel_actions.paid_acceptance_ea.supabase_writer import (
    build_products_maps,
    split_known_unknown_nmids,
    upsert_paid_acceptance,
)


class TestPaidAcceptanceValidation:
    """Тесты для валидации структуры Paid Acceptance API"""
    
    def test_valid_response_passes(self, load_fixture):
        """Валидный ответ должен проходить валидацию"""
        data = load_fixture('paid_acceptance/paid_acceptance_valid.json')
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Валидация должна пройти без исключения
        validate_acceptance_response(data)
    
    def test_empty_array_passes(self, load_fixture):
        """Пустой массив - валидный случай"""
        data = load_fixture('paid_acceptance/paid_acceptance_empty.json')
        
        assert data == []
        
        # Валидация должна пройти
        validate_acceptance_response(data)
    
    def test_not_array_fails(self, load_fixture):
        """Ответ не является массивом - должна быть ошибка"""
        data = load_fixture('paid_acceptance/paid_acceptance_not_array.json')
        
        assert not isinstance(data, list)
        
        with pytest.raises(ValidationError) as exc_info:
            validate_acceptance_response(data)
        
        assert "array" in str(exc_info.value).lower()
    
    def test_missing_shkCreateDate_fails(self, load_fixture):
        """Отсутствие поля shkCreateDate - должна быть ошибка"""
        data = load_fixture('paid_acceptance/paid_acceptance_missing_shkCreateDate.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_acceptance_response(data)
        
        assert "shkcreatedate" in str(exc_info.value).lower()
    
    def test_missing_nmID_fails(self, load_fixture):
        """Отсутствие поля nmID - должна быть ошибка"""
        data = load_fixture('paid_acceptance/paid_acceptance_missing_nmID.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_acceptance_response(data)
        
        assert "nmid" in str(exc_info.value).lower()
    
    def test_missing_count_fails(self, load_fixture):
        """Отсутствие поля count - должна быть ошибка"""
        data = load_fixture('paid_acceptance/paid_acceptance_missing_count.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_acceptance_response(data)
        
        assert "count" in str(exc_info.value).lower()
    
    def test_missing_total_fails(self, load_fixture):
        """Отсутствие поля total - должна быть ошибка"""
        data = load_fixture('paid_acceptance/paid_acceptance_missing_total.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_acceptance_response(data)
        
        assert "total" in str(exc_info.value).lower()
    
    def test_wrong_type_nmID_fails(self, load_fixture):
        """nmID должен быть integer"""
        data = load_fixture('paid_acceptance/paid_acceptance_wrong_type_nmID.json')
        
        with pytest.raises(ValidationError) as exc_info:
            validate_acceptance_response(data)
        
        assert "nmid" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower()
    
    def test_get_validation_report_valid(self, load_fixture):
        """Отчёт валидации для валидных данных"""
        data = load_fixture('paid_acceptance/paid_acceptance_valid.json')
        
        report = get_validation_report(data)
        
        assert report['valid'] == True
        assert report['errors'] == []
        assert report['error_count'] == 0
    
    def test_get_validation_report_invalid(self, load_fixture):
        """Отчёт валидации для некорректных данных"""
        data = load_fixture('paid_acceptance/paid_acceptance_missing_shkCreateDate.json')
        
        report = get_validation_report(data)
        
        assert report['valid'] == False
        assert len(report['errors']) > 0
        assert report['error_count'] > 0


class TestPaidAcceptanceTransform:
    """Тесты для агрегации данных"""
    
    def test_aggregate_by_date_and_nm_id(self, load_fixture):
        """Агрегация по (shkCreateDate, nmID) с суммированием count и total"""
        data = load_fixture('paid_acceptance/paid_acceptance_valid.json')
        
        result = aggregate_acceptance(data)
        
        # Должно быть 3 записи: (2025-08-09, 123456), (2025-08-09, 789012), (2025-08-15, 789012)
        assert len(result) == 3
        
        # Проверяем первую запись: 2025-08-09, 123456 (100 + 51 = 151 count, 373.00 + 127.50 = 500.50 total)
        r1 = next(r for r in result if r['shk_create_date'] == '2025-08-09' and r['nm_id'] == 123456)
        assert r1['count'] == 151
        assert r1['total'] == Decimal('500.50')
        
        # Проверяем вторую запись: 2025-08-09, 789012
        r2 = next(r for r in result if r['shk_create_date'] == '2025-08-09' and r['nm_id'] == 789012)
        assert r2['count'] == 20
        assert r2['total'] == Decimal('204.00')
        
        # Проверяем третью запись: 2025-08-15, 789012
        r3 = next(r for r in result if r['shk_create_date'] == '2025-08-15' and r['nm_id'] == 789012)
        assert r3['count'] == 30
        assert r3['total'] == Decimal('150.00')
    
    def test_aggregate_rounding(self):
        """Проверка округления до 2 знаков"""
        data = [
            {"shkCreateDate": "2025-08-09", "nmID": 123, "count": 5, "total": 1.2345}
        ]
        
        result = aggregate_acceptance(data)
        
        assert len(result) == 1
        assert result[0]['total'] == Decimal('1.23')
    
    def test_per_article_logs(self):
        """Проверка форматирования логов по артикулу"""
        aggregated = [
            {'shk_create_date': '2025-08-09', 'nm_id': 123456, 'count': 100, 'total': Decimal('373.00')},
            {'shk_create_date': '2025-08-09', 'nm_id': 789012, 'count': 20, 'total': Decimal('204.00')},
        ]
        nm_to_vendor = {123456: 'test_vendor_1', 789012: 'test_vendor_2'}
        
        logs = per_article_logs(aggregated, nm_to_vendor)
        
        assert len(logs) == 2
        assert 'test_vendor_1' in logs[0]
        assert '2025-08-09' in logs[0]
        assert '100' in logs[0]
        assert '373.00' in logs[0]
        assert '3.73' in logs[0]  # price_per_unit


class TestPaidAcceptanceWriter:
    """Тесты для записи в Supabase"""
    
    def test_build_products_maps(self):
        """Создание маппингов nm_id -> product_id и nm_id -> vendor_code"""
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.execute.return_value.data = [
            {'nm_id': 123456, 'id': 'uuid-123', 'vendor_code': 'vendor1'},
            {'nm_id': 789012, 'id': 'uuid-789', 'vendor_code': 'vendor2'},
        ]
        
        nm_to_product, nm_to_vendor = build_products_maps(mock_supabase)
        
        assert nm_to_product == {123456: 'uuid-123', 789012: 'uuid-789'}
        assert nm_to_vendor == {123456: 'vendor1', 789012: 'vendor2'}
        mock_supabase.table.assert_called_once_with('products')
    
    def test_split_known_unknown_nmids(self):
        """Разделение на известные и неизвестные nm_id"""
        aggregated = [
            {'nm_id': 123456, 'shk_create_date': '2025-08-09', 'count': 100, 'total': 373.0},
            {'nm_id': 789012, 'shk_create_date': '2025-08-09', 'count': 20, 'total': 204.0},
            {'nm_id': 999999, 'shk_create_date': '2025-08-09', 'count': 30, 'total': 150.0},
        ]
        products_map = {123456: 'uuid-123', 789012: 'uuid-789'}
        vendor_map = {123456: 'vendor1', 789012: 'vendor2'}
        
        known, missing = split_known_unknown_nmids(aggregated, products_map, vendor_map)
        
        assert len(known) == 2
        assert len(missing) == 1
        assert 999999 in missing
        
        # Проверяем обогащение product_id и vendor_code
        assert known[0]['product_id'] == 'uuid-123'
        assert known[0]['vendor_code'] == 'vendor1'
        assert known[1]['product_id'] == 'uuid-789'
        assert known[1]['vendor_code'] == 'vendor2'
        
        # Проверяем что Decimal конвертирован в float
        assert isinstance(known[0]['total'], float)
    
    def test_upsert_paid_acceptance_new_records(self):
        """Upsert новых записей"""
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.gte.return_value.lte.return_value.execute.return_value.data = []
        
        rows = [
            {
                'nm_id': 123456,
                'shk_create_date': '2025-08-09',
                'vendor_code': 'vendor1',
                'count': 100,
                'total': 373.0,
                'product_id': 'uuid-123'
            },
        ]
        
        mock_supabase.table.return_value.upsert.return_value.execute.return_value.data = rows
        
        processed, updated = upsert_paid_acceptance(rows, mock_supabase)
        
        assert processed == 1
        assert len(updated) == 0  # новые записи, не обновления
        mock_supabase.table.return_value.upsert.assert_called_once()
    
    def test_upsert_paid_acceptance_updated_records(self):
        """Upsert с обновлением изменённых записей (count или total изменились)"""
        mock_supabase = MagicMock()
        # Симулируем существующую запись
        mock_supabase.table.return_value.select.return_value.gte.return_value.lte.return_value.execute.return_value.data = [
            {'nm_id': 123456, 'shk_create_date': '2025-08-09', 'count': 50, 'total': 186.5}
        ]
        
        rows = [
            {
                'nm_id': 123456,
                'shk_create_date': '2025-08-09',
                'vendor_code': 'vendor1',
                'count': 100,
                'total': 373.0,
                'product_id': 'uuid-123'
            },
        ]
        
        mock_supabase.table.return_value.upsert.return_value.execute.return_value.data = rows
        
        processed, updated = upsert_paid_acceptance(rows, mock_supabase)
        
        assert processed == 1
        assert len(updated) == 1  # одна запись обновлена
        assert updated[0]['nm_id'] == 123456
        assert updated[0]['old_count'] == 50
        assert updated[0]['new_count'] == 100
        assert updated[0]['old_total'] == 186.5
        assert updated[0]['new_total'] == 373.0
    
    def test_upsert_paid_acceptance_no_changes(self):
        """Upsert без изменений - не обновляет"""
        mock_supabase = MagicMock()
        # Симулируем существующую запись с теми же значениями
        mock_select_chain = MagicMock()
        mock_select_chain.execute.return_value.data = [
            {'nm_id': 123456, 'shk_create_date': '2025-08-09', 'count': 100, 'total': 373.0}
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
            {
                'nm_id': 123456,
                'shk_create_date': '2025-08-09',
                'vendor_code': 'vendor1',
                'count': 100,
                'total': 373.0,
                'product_id': 'uuid-123'
            },
        ]
        
        processed, updated = upsert_paid_acceptance(rows, mock_supabase)
        
        # Если count и total не изменились, to_write будет пустым, обработано 0
        assert processed == 0
        assert len(updated) == 0
    
    def test_upsert_paid_acceptance_only_count_changed(self):
        """Upsert когда изменился только count, а total остался прежним - должно обновить"""
        mock_supabase = MagicMock()
        # Существующая запись с другим count, но тем же total
        mock_supabase.table.return_value.select.return_value.gte.return_value.lte.return_value.execute.return_value.data = [
            {'nm_id': 123456, 'shk_create_date': '2025-08-09', 'count': 50, 'total': 373.0}
        ]
        
        rows = [
            {
                'nm_id': 123456,
                'shk_create_date': '2025-08-09',
                'vendor_code': 'vendor1',
                'count': 100,
                'total': 373.0,
                'product_id': 'uuid-123'
            },
        ]
        
        mock_supabase.table.return_value.upsert.return_value.execute.return_value.data = rows
        
        processed, updated = upsert_paid_acceptance(rows, mock_supabase)
        
        assert processed == 1
        assert len(updated) == 1  # должно обновиться, т.к. count изменился
    
    def test_upsert_paid_acceptance_empty_list(self):
        """Upsert пустого списка"""
        mock_supabase = MagicMock()
        
        processed, updated = upsert_paid_acceptance([], mock_supabase)
        
        assert processed == 0
        assert len(updated) == 0
        # Не должно быть вызовов к БД
        assert not hasattr(mock_supabase.table, 'call_count') or mock_supabase.table.call_count == 0

