"""
Общие фикстуры для всех тестов проекта.

Этот файл автоматически загружается pytest и предоставляет фикстуры
для всех тестов без необходимости явного импорта.
"""
import pytest
import json
from pathlib import Path


@pytest.fixture
def load_fixture():
    """
    Фикстура для загрузки JSON файлов из директории fixtures/.
    
    Использование в тестах:
        def test_example(load_fixture):
            # Загрузка из подпапки модуля
            data = load_fixture('cr_daily_stats/cr_valid.json')
            assert 'data' in data
    
    Returns:
        Функция, которая принимает путь к файлу (с подпапкой) и возвращает распарсенный JSON
    """
    def _load(filepath: str) -> dict:
        """
        Args:
            filepath: Путь к фикстуре относительно fixtures/
                     Например: 'cr_daily_stats/cr_valid.json'
        """
        fixture_path = Path(__file__).parent / "fixtures" / filepath
        
        if not fixture_path.exists():
            raise FileNotFoundError(f"Fixture not found: {fixture_path}")
        
        with open(fixture_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    return _load


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """
    Hook для вывода итоговой информации после всех тестов.
    Показывает статистику и ссылку на HTML отчёт coverage.
    """
    print("\n" + "=" * 70)
    print("📊 ИТОГИ ТЕСТИРОВАНИЯ")
    print("=" * 70)
    
    # Статистика тестов
    passed = len(terminalreporter.stats.get('passed', []))
    failed = len(terminalreporter.stats.get('failed', []))
    total = passed + failed
    
    if total > 0:
        print(f"✅ Пройдено: {passed}/{total}")
        print(f"❌ Провалено: {failed}/{total}")
        
        if failed == 0:
            print("🎉 Все тесты прошли успешно!")
    
    # Информация о coverage
    if hasattr(config.option, 'cov_report') and config.option.cov_report:
        print(f"\n📈 Coverage отчёт создан")
        print(f"📁 HTML отчёт: tests/htmlcov/index.html")
        print(f"💡 Открыть: open tests/htmlcov/index.html")
    
    print("=" * 70)