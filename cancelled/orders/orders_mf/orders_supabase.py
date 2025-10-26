"""
Главная функция для обработки заказов Wildberries.
"""

import sys
import logging
import importlib.util
from pathlib import Path

# Базовый путь к проекту
BASE_DIR = Path(__file__).resolve().parents[2]

# Импорт API клиента
wb_api_path = BASE_DIR / 'wb_api' / 'orders' / 'orders.py'
spec = importlib.util.spec_from_file_location("orders", str(wb_api_path))
orders_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(orders_module)
WildberriesOrdersAPI = orders_module.WildberriesOrdersAPI

# Импорт валидатора структуры
structure_validator_path = BASE_DIR / 'excel_actions' / 'orders_ea' / 'structure_validator.py'
spec_validator = importlib.util.spec_from_file_location("structure_validator", str(structure_validator_path))
validator_module = importlib.util.module_from_spec(spec_validator)
spec_validator.loader.exec_module(validator_module)
check_and_validate_structure = validator_module.check_and_validate_structure

# Импорт writer
supabase_writer_path = BASE_DIR / 'excel_actions' / 'orders_ea' / 'supabase_writer.py'
spec_writer = importlib.util.spec_from_file_location("supabase_writer", str(supabase_writer_path))
writer_module = importlib.util.module_from_spec(spec_writer)
spec_writer.loader.exec_module(writer_module)
write_orders_to_supabase = writer_module.write_orders_to_supabase

# Импорт API ключей
api_keys_path = BASE_DIR / 'api_keys.py'
spec_keys = importlib.util.spec_from_file_location("api_keys", str(api_keys_path))
api_keys_module = importlib.util.module_from_spec(spec_keys)
spec_keys.loader.exec_module(api_keys_module)

# Импорт Supabase
from supabase import create_client, Client
SUPABASE_URL = api_keys_module.SUPABASE_URL
SUPABASE_KEY = api_keys_module.SUPABASE_KEY

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Отключаем HTTP логирование от Supabase
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def connect_to_supabase() -> Client:
    """Подключается к Supabase."""
    try:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError(
                "SUPABASE_URL or SUPABASE_KEY not set in api_keys.py. "
                "Check SUPABASE_URL and SUPABASE_KEY in api_keys.py"
            )
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info(f"✅ Подключено к Supabase: {SUPABASE_URL}")
        return supabase
        
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Supabase: {e}")
        raise


def main():
    """Основная функция оркестрации."""
    
    print("\n" + "=" * 80)
    print("🚀 ОРКЕСТРАЦИЯ: ЗАКАЗЫ WILDBERRIES")
    print("=" * 80 + "\n")
    
    # Шаг 1: Подключение к БД
    print("📡 Подключение к Supabase...")
    supabase = connect_to_supabase()
    
    # Шаг 2: Получение данных из API
    print("\n" + "=" * 80)
    print("📥 ШАГ 1: ПОЛУЧЕНИЕ ДАННЫХ ИЗ API")
    print("=" * 80 + "\n")
    
    api = WildberriesOrdersAPI()
    
    try:
        orders = api.fetch_orders()
        logger.info(f"✅ Получено заказов из API: {len(orders)}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения данных из API: {e}")
        return
    
    # Шаг 3: Валидация структуры
    print("\n" + "=" * 80)
    print("🔍 ШАГ 2: ВАЛИДАЦИЯ СТРУКТУРЫ")
    print("=" * 80 + "\n")
    
    is_valid = check_and_validate_structure(orders)
    
    if not is_valid:
        logger.error("❌ Валидация не пройдена. Остановка выполнения.")
        return
    
    # Шаг 4: Запись в БД
    print("\n" + "=" * 80)
    print("💾 ШАГ 3: ЗАПИСЬ В БД")
    print("=" * 80 + "\n")
    
    try:
        written_count = write_orders_to_supabase(orders, supabase)
        logger.info(f"✅ Записано заказов в БД: {written_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка записи в БД: {e}")
        return
    
    # Итоги
    print("\n" + "=" * 80)
    print("✅ ИТОГИ")
    print("=" * 80)
    print(f"Получено заказов: {len(orders)}")
    print(f"Записано в БД: {written_count}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
