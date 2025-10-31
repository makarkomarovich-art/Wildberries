"""
Тесты для CR Daily Stats (Conversion Rate статистика).

Тестируем:
1. Валидацию структуры ответа API
2. Обработку edge cases (пустые данные, отсутствующие поля)
3. Корректность валидаторов
"""
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для импортов
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from excel_actions.cr_daily_stats_ea.structure_validator import validate_cr_daily_stats_structure


class TestCRDailyStatsValidation:
    """Тесты для валидации структуры CR Daily Stats"""
    
    def test_valid_response_passes_validation(self, load_fixture):
        """
        Тест #1: Валидный ответ от API должен проходить валидацию.
        
        Проверяем:
        - Все обязательные поля присутствуют
        - Типы данных корректны
        - Валидатор возвращает True
        """
        # Загружаем валидный ответ
        data = load_fixture('cr_daily_stats/cr_valid.json')
        
        # Проверяем что есть карточки
        assert 'data' in data
        assert 'cards' in data['data']
        assert len(data['data']['cards']) > 0
        
        # Валидация должна пройти
        result = validate_cr_daily_stats_structure(data)
        assert result == True, "Валидный ответ должен проходить валидацию"
    
    def test_empty_cards_array_passes_with_warning(self, load_fixture):
        """
        Тест #2: Пустой массив карточек должен проходить валидацию с предупреждением.
        
        Проверяем:
        - API вернул корректную структуру, но без карточек
        - Валидатор возвращает True (это не ошибка, просто нет данных)
        - В выводе есть предупреждение
        """
        # Загружаем ответ с пустым массивом
        data = load_fixture('cr_daily_stats/cr_empty.json')
        
        # Проверяем структуру
        assert 'data' in data
        assert 'cards' in data['data']
        assert len(data['data']['cards']) == 0
        
        # Валидация должна пройти (это не ошибка)
        result = validate_cr_daily_stats_structure(data)
        assert result == True, "Пустой массив карточек не является ошибкой"
    
    def test_missing_nmID_fails_validation(self, load_fixture):
        """
        Тест #3: Отсутствие nmID в карточке должно провалить валидацию.
        
        Проверяем:
        - nmID - критичное поле (без него нельзя идентифицировать артикул)
        - Валидатор возвращает False
        - В выводе есть сообщение об ошибке
        """
        # Загружаем ответ без nmID
        data = load_fixture('cr_daily_stats/cr_missing_nmID.json')
        
        # Проверяем что структура есть, но nmID отсутствует
        assert 'data' in data
        assert 'cards' in data['data']
        assert len(data['data']['cards']) > 0
        
        first_card = data['data']['cards'][0]
        assert 'nmID' not in first_card, "В фикстуре не должно быть nmID"
        
        # Валидация должна провалиться
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие nmID должно провалить валидацию"


    def test_missing_data_field_fails_validation(self, load_fixture):
        """
        Тест #4: Отсутствие поля 'data' должно провалить валидацию.
        
        Проверяем:
        - data - корневое обязательное поле
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_data.json')
        
        assert 'data' not in data
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие 'data' должно провалить валидацию"
    
    def test_wrong_type_nmID_fails_validation(self, load_fixture):
        """
        Тест #5: Неправильный тип nmID (string вместо integer) должен провалить валидацию.
        
        Проверяем:
        - nmID должен быть integer
        - Если string - валидация должна провалиться
        """
        data = load_fixture('cr_daily_stats/cr_wrong_type_nmID.json')
        
        first_card = data['data']['cards'][0]
        assert isinstance(first_card['nmID'], str), "nmID должен быть строкой в фикстуре"
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "nmID типа string должен провалить валидацию"
    
    def test_missing_vendorCode_fails_validation(self, load_fixture):
        """
        Тест #6: Отсутствие vendorCode должно провалить валидацию.
        
        Проверяем:
        - vendorCode - обязательное поле
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_vendorCode.json')
        
        first_card = data['data']['cards'][0]
        assert 'vendorCode' not in first_card
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие vendorCode должно провалить валидацию"
    
    def test_missing_selectedPeriod_fails_validation(self, load_fixture):
        """
        Тест #7: Отсутствие selectedPeriod должно провалить валидацию.
        
        Проверяем:
        - selectedPeriod - обязательное поле в statistics
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_selectedPeriod.json')
        
        stats = data['data']['cards'][0]['statistics']
        assert 'selectedPeriod' not in stats
        assert 'previousPeriod' in stats  # но previousPeriod есть
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие selectedPeriod должно провалить валидацию"
    
    def test_missing_conversions_fails_validation(self, load_fixture):
        """
        Тест #8: Отсутствие conversions в периоде должно провалить валидацию.
        
        Проверяем:
        - conversions - обязательное поле в каждом периоде
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_conversions.json')
        
        selected_period = data['data']['cards'][0]['statistics']['selectedPeriod']
        assert 'conversions' not in selected_period
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие conversions должно провалить валидацию"


    def test_response_not_dict_fails_validation(self, load_fixture):
        """
        Тест #9: Ответ API не является объектом (строка).
        
        Проверяем:
        - response_data должен быть dict
        - Если строка/число - валидация должна провалиться
        """
        data = load_fixture('cr_daily_stats/cr_not_dict.json')
        
        assert isinstance(data, str), "Должна быть строка в фикстуре"
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Ответ-строка должен провалить валидацию"
    
    def test_data_not_dict_fails_validation(self, load_fixture):
        """
        Тест #10: Поле 'data' не является объектом.
        
        Проверяем:
        - data должен быть dict
        - Если строка - валидация должна провалиться
        """
        data = load_fixture('cr_daily_stats/cr_data_not_dict.json')
        
        assert isinstance(data['data'], str), "data должен быть строкой в фикстуре"
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "data-строка должен провалить валидацию"
    
    def test_missing_cards_field_fails_validation(self, load_fixture):
        """
        Тест #11: Отсутствует поле 'cards' в data.
        
        Проверяем:
        - cards - обязательное поле в data
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_cards.json')
        
        assert 'cards' not in data['data']
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие 'cards' должно провалить валидацию"
    
    def test_cards_not_array_fails_validation(self, load_fixture):
        """
        Тест #12: Поле 'cards' не является массивом.
        
        Проверяем:
        - cards должен быть list/array
        - Если строка - валидация должна провалиться
        """
        data = load_fixture('cr_daily_stats/cr_cards_not_array.json')
        
        assert isinstance(data['data']['cards'], str), "cards должен быть строкой в фикстуре"
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "cards-строка должен провалить валидацию"


    def test_wrong_type_vendorCode_fails_validation(self, load_fixture):
        """
        Тест #13: vendorCode неправильного типа (число вместо строки).
        
        Проверяем:
        - vendorCode должен быть string
        - Если число - валидация должна провалиться
        """
        data = load_fixture('cr_daily_stats/cr_wrong_type_vendorCode.json')
        
        vendor_code = data['data']['cards'][0]['vendorCode']
        assert isinstance(vendor_code, int), "vendorCode должен быть числом в фикстуре"
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "vendorCode-число должен провалить валидацию"
    
    def test_missing_statistics_fails_validation(self, load_fixture):
        """
        Тест #14: Отсутствует поле 'statistics'.
        
        Проверяем:
        - statistics - обязательное поле в карточке
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_statistics.json')
        
        card = data['data']['cards'][0]
        assert 'statistics' not in card
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие statistics должно провалить валидацию"
    
    def test_statistics_not_dict_fails_validation(self, load_fixture):
        """
        Тест #15: Поле 'statistics' не является объектом.
        
        Проверяем:
        - statistics должен быть dict
        - Если строка - валидация должна провалиться
        """
        data = load_fixture('cr_daily_stats/cr_statistics_not_dict.json')
        
        stats = data['data']['cards'][0]['statistics']
        assert isinstance(stats, str), "statistics должен быть строкой в фикстуре"
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "statistics-строка должен провалить валидацию"


    def test_missing_previousPeriod_fails_validation(self, load_fixture):
        """
        Тест #16: Отсутствует previousPeriod.
        
        Проверяем:
        - previousPeriod - обязательное поле в statistics
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_previousPeriod.json')
        
        stats = data['data']['cards'][0]['statistics']
        assert 'previousPeriod' not in stats
        assert 'selectedPeriod' in stats  # но selectedPeriod есть
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие previousPeriod должно провалить валидацию"
    
    def test_missing_stocks_fails_validation(self, load_fixture):
        """
        Тест #17: Отсутствует поле 'stocks'.
        
        Проверяем:
        - stocks - обязательное поле в карточке
        - Валидатор возвращает False
        """
        data = load_fixture('cr_daily_stats/cr_missing_stocks.json')
        
        card = data['data']['cards'][0]
        assert 'stocks' not in card
        
        result = validate_cr_daily_stats_structure(data)
        assert result == False, "Отсутствие stocks должно провалить валидацию"


