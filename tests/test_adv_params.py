"""
Тесты для ADV Params (Рекламная статистика).

Тестируем:
1. Валидацию ответа API /adv/v1/promotion/count
2. Валидацию ответа API /adv/v3/fullstats
3. JSON Schema валидацию
"""
import sys
from pathlib import Path
import pytest

# Добавляем корень проекта в sys.path для импортов
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from excel_actions.adv_params_ea.structure_validator import (
    validate_promotion_count_response,
    validate_fullstats_response,
    get_validation_report,
    load_schema,
    ValidationError,
    PROMOTION_COUNT_SCHEMA,
    FULLSTATS_SCHEMA
)


class TestPromotionCountValidation:
    """Тесты для валидации /adv/v1/promotion/count API"""
    
    def test_valid_promotion_count_passes(self, load_fixture):
        """
        Тест #1: Валидный ответ promotion/count должен проходить валидацию.
        
        Проверяем:
        - Корректная структура с adverts и all
        - Валидация проходит без исключений
        """
        data = load_fixture('adv_params/promotion_count_valid.json')
        
        # Проверяем структуру
        assert 'adverts' in data
        assert 'all' in data
        assert isinstance(data['adverts'], list)
        
        # Валидация должна пройти (не выбросить исключение)
        validate_promotion_count_response(data)
    
    def test_empty_promotion_count_passes(self, load_fixture):
        """
        Тест #2: Пустой ответ promotion/count (нет активных кампаний).
        
        Проверяем:
        - Пустой массив adverts - валидный случай
        - all = 0
        - Валидация проходит
        """
        data = load_fixture('adv_params/promotion_count_empty.json')
        
        assert data['adverts'] == []
        assert data['all'] == 0
        
        # Валидация должна пройти
        validate_promotion_count_response(data)
    
    def test_missing_adverts_field_fails(self, load_fixture):
        """
        Тест #3: Отсутствует обязательное поле 'adverts'.
        
        Проверяем:
        - adverts - обязательное поле
        - Валидация выбрасывает ValidationError
        """
        data = load_fixture('adv_params/promotion_count_missing_adverts.json')
        
        assert 'adverts' not in data
        
        # Валидация должна выбросить исключение
        with pytest.raises(ValidationError) as exc_info:
            validate_promotion_count_response(data)
        
        assert "validation failed" in str(exc_info.value).lower()


class TestFullstatsValidation:
    """Тесты для валидации /adv/v3/fullstats API"""
    
    def test_valid_fullstats_passes(self, load_fixture):
        """
        Тест #4: Валидный ответ fullstats должен проходить валидацию.
        
        Проверяем:
        - Массив кампаний с полной структурой
        - Вложенные days[] и apps[]
        - Все обязательные поля присутствуют
        """
        data = load_fixture('adv_params/fullstats_valid.json')
        
        # Проверяем структуру
        assert isinstance(data, list)
        assert len(data) > 0
        
        first_campaign = data[0]
        assert 'advertId' in first_campaign
        assert 'days' in first_campaign
        assert isinstance(first_campaign['days'], list)
        
        # Валидация должна пройти
        validate_fullstats_response(data)
    
    def test_empty_fullstats_array_passes(self, load_fixture):
        """
        Тест #5: Пустой массив fullstats (нет кампаний).
        
        Проверяем:
        - Пустой массив [] - валидный случай
        - Валидация проходит
        """
        data = load_fixture('adv_params/fullstats_empty.json')
        
        assert data == []
        
        # Валидация должна пройти
        validate_fullstats_response(data)
    
    def test_fullstats_not_array_fails(self, load_fixture):
        """
        Тест #6: Ответ fullstats не является массивом (объект вместо массива).
        
        Проверяем:
        - Корень должен быть array, не object
        - Валидация выбрасывает ValidationError
        """
        data = load_fixture('adv_params/fullstats_not_array.json')
        
        assert isinstance(data, dict), "Должен быть объект в фикстуре"
        
        # Валидация должна выбросить исключение
        with pytest.raises(ValidationError) as exc_info:
            validate_fullstats_response(data)
        
        assert "validation failed" in str(exc_info.value).lower()
    
    def test_missing_advertId_fails(self, load_fixture):
        """
        Тест #7: Отсутствует обязательное поле 'advertId'.
        
        Проверяем:
        - advertId - обязательное поле в каждой кампании
        - JSON Schema выбрасывает ValidationError
        """
        data = load_fixture('adv_params/fullstats_missing_advertId.json')
        
        assert 'advertId' not in data[0]
        
        # Валидация должна выбросить исключение
        with pytest.raises(ValidationError) as exc_info:
            validate_fullstats_response(data)
        
        assert "validation failed" in str(exc_info.value).lower()
    
    def test_wrong_type_clicks_fails(self, load_fixture):
        """
        Тест #8: Неправильный тип поля clicks (string вместо integer).
        
        Проверяем:
        - clicks должен быть integer
        - JSON Schema отклоняет string
        """
        data = load_fixture('adv_params/fullstats_wrong_type_clicks.json')
        
        assert isinstance(data[0]['clicks'], str), "clicks должен быть строкой в фикстуре"
        
        # Валидация должна выбросить исключение
        with pytest.raises(ValidationError):
            validate_fullstats_response(data)
    
    def test_negative_views_fails(self, load_fixture):
        """
        Тест #9: Отрицательное значение views (нарушает minimum: 0).
        
        Проверяем:
        - views должен быть >= 0 (по JSON Schema)
        - Отрицательное значение отклоняется
        """
        data = load_fixture('adv_params/fullstats_negative_views.json')
        
        assert data[0]['views'] < 0, "views должен быть отрицательным в фикстуре"
        
        # Валидация должна выбросить исключение
        with pytest.raises(ValidationError):
            validate_fullstats_response(data)
    
    def test_missing_days_field_fails(self, load_fixture):
        """
        Тест #10: Отсутствует обязательное поле 'days'.
        
        Проверяем:
        - days - обязательное поле (детализация по дням)
        - JSON Schema выбрасывает ValidationError
        """
        data = load_fixture('adv_params/fullstats_missing_days.json')
        
        assert 'days' not in data[0]
        
        # Валидация должна выбросить исключение
        with pytest.raises(ValidationError):
            validate_fullstats_response(data)


class TestValidationHelpers:
    """Тесты для вспомогательных функций валидации"""
    
    def test_load_schema_success(self):
        """
        Тест #11: Загрузка существующей схемы.
        
        Проверяем:
        - Схема загружается корректно
        - Возвращается dict
        """
        schema = load_schema(FULLSTATS_SCHEMA)
        
        assert isinstance(schema, dict)
        assert '$schema' in schema or 'type' in schema
    
    def test_load_schema_file_not_found(self):
        """
        Тест #12: Схема не найдена - выбрасывает FileNotFoundError.
        
        Проверяем:
        - Если файл схемы не существует
        - Выбрасывается FileNotFoundError
        """
        from pathlib import Path
        
        fake_path = Path("/fake/path/schema.json")
        
        with pytest.raises(FileNotFoundError):
            load_schema(fake_path)
    
    def test_get_validation_report_valid_data(self, load_fixture):
        """
        Тест #13: Отчёт валидации для корректных данных.
        
        Проверяем:
        - valid = True
        - errors = []
        - error_count = 0
        """
        data = load_fixture('adv_params/fullstats_valid.json')
        schema = load_schema(FULLSTATS_SCHEMA)
        
        report = get_validation_report(data, schema)
        
        assert report['valid'] == True
        assert report['errors'] == []
        assert report['error_count'] == 0
    
    def test_get_validation_report_invalid_data(self, load_fixture):
        """
        Тест #14: Отчёт валидации для некорректных данных.
        
        Проверяем:
        - valid = False
        - errors содержит список ошибок
        - error_count > 0
        """
        data = load_fixture('adv_params/fullstats_missing_advertId.json')
        schema = load_schema(FULLSTATS_SCHEMA)
        
        report = get_validation_report(data, schema)
        
        assert report['valid'] == False
        assert len(report['errors']) > 0
        assert report['error_count'] > 0
        assert isinstance(report['errors'], list)


# Итого: 14 тестов
# Target: 80%+ coverage для structure_validator.py


