"""
Main: сбор базы артикулов из Content API (cards list) и запись в Supabase.

Поля для products: nmID, imtID, vendorCode, title, subjectName
Поля для product_sizes: barcode, size (techSize)

Логика:
1. Получить карточки из WB API (с пагинацией)
2. Валидация структуры (обязательные поля)
3. Нормализация данных для Supabase
4. Применение исключений (excluded_nm_ids)
5. Upsert в Supabase (products + product_sizes)
"""

from __future__ import annotations

from pathlib import Path
import importlib.util
from typing import Any, Dict, List


BASE_DIR = Path(__file__).resolve().parents[2]

# ========================================
# Импорты
# ========================================

# WB API Client
content_api_path = BASE_DIR / 'wb_api' / 'content_cards.py'
spec = importlib.util.spec_from_file_location('content_cards', str(content_api_path))
content_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(content_mod)
WBContentCardsClient = content_mod.WBContentCardsClient

# API keys
api_keys_path = BASE_DIR / 'api_keys.py'
spec_keys = importlib.util.spec_from_file_location('api_keys', str(api_keys_path))
ak = importlib.util.module_from_spec(spec_keys)
spec_keys.loader.exec_module(ak)
WB_API_TOKEN = ak.WB_API_TOKEN
SUPABASE_URL = ak.SUPABASE_URL_LOCAL
SUPABASE_KEY = ak.SUPABASE_SERVICE_KEY_LOCAL

# Валидатор структуры
struct_path = BASE_DIR / 'excel_actions' / 'list_of_seller_articles_ea' / 'structure_validator.py'
spec_struct = importlib.util.spec_from_file_location('structure_validator', str(struct_path))
struct_mod = importlib.util.module_from_spec(spec_struct)
spec_struct.loader.exec_module(struct_mod)
check_and_validate_structure = struct_mod.check_and_validate_structure

# Нормализация данных
norm_path = BASE_DIR / 'excel_actions' / 'list_of_seller_articles_ea' / 'normalize_articles.py'
spec_norm = importlib.util.spec_from_file_location('normalize_articles', str(norm_path))
norm_mod = importlib.util.module_from_spec(spec_norm)
spec_norm.loader.exec_module(norm_mod)
extract_data_for_supabase = norm_mod.extract_data_for_supabase

# Исключения
excluded_path = BASE_DIR / 'main_function' / 'list_of_seller_articles_mf' / 'excluded_nm_ids.py'
spec_excl = importlib.util.spec_from_file_location('excluded_nm_ids', str(excluded_path))
excl_mod = importlib.util.module_from_spec(spec_excl)
spec_excl.loader.exec_module(excl_mod)
EXCLUDED_NM_IDS = excl_mod.EXCLUDED_NM_IDS

# Supabase
try:
    from supabase import create_client, Client
except ImportError:
    print("❌ ОШИБКА: Библиотека supabase не установлена")
    print("   Установите: pip install supabase")
    exit(1)


def upsert_to_supabase(
    supabase: Client,
    products_data: List[Dict[str, Any]],
    product_sizes_data: List[Dict[str, Any]]
) -> None:
    """
    Записывает данные в Supabase с upsert-логикой.
    
    - products: upsert по nm_id (UUID сохраняется)
    - product_sizes: upsert по barcode
    """
    print(f"\n📤 Запись в Supabase...")
    
    # 1. Upsert products
    if products_data:
        print(f"   Товаров для upsert: {len(products_data)}")
        try:
            result = supabase.table('products').upsert(
                products_data,
                on_conflict='nm_id'
            ).execute()
            print(f"   ✅ Products: обработано {len(result.data)} записей")
        except Exception as e:
            print(f"   ❌ ОШИБКА при записи products: {e}")
            raise
    
    # 2. Получаем UUID для каждого nm_id (для связи с product_sizes)
    nm_id_to_uuid = {}
    if products_data:
        nm_ids = [p['nm_id'] for p in products_data]
        try:
            result = supabase.table('products').select('id, nm_id').in_('nm_id', nm_ids).execute()
            nm_id_to_uuid = {row['nm_id']: row['id'] for row in result.data}
        except Exception as e:
            print(f"   ❌ ОШИБКА при получении UUID товаров: {e}")
            raise
    
    # 3. Добавляем product_id в product_sizes_data
    if product_sizes_data:
        print(f"   Размеров/баркодов для upsert: {len(product_sizes_data)}")
        for size_item in product_sizes_data:
            nm_id = size_item.pop('nm_id')  # убираем nm_id, добавляем product_id
            product_uuid = nm_id_to_uuid.get(nm_id)
            if not product_uuid:
                print(f"   ⚠️ WARNING: Не найден UUID для nm_id={nm_id}, пропускаем барк од {size_item.get('barcode')}")
                continue
            size_item['product_id'] = product_uuid
        
        # Фильтруем записи без product_id
        product_sizes_data = [s for s in product_sizes_data if 'product_id' in s]
        
        if product_sizes_data:
            try:
                result = supabase.table('product_sizes').upsert(
                    product_sizes_data,
                    on_conflict='barcode'
                ).execute()
                print(f"   ✅ Product sizes: обработано {len(result.data)} записей")
            except Exception as e:
                print(f"   ❌ ОШИБКА при записи product_sizes: {e}")
                raise
    
    print("✅ Запись в Supabase завершена")


def main() -> None:
    print("🚀 Старт сборки базы артикулов (Content API → Supabase)")
    
    # Диагностика
    def _mask(v: str) -> str:
        if not v:
            return "<empty>"
        return (v[:12] + "..." + v[-12:]) if len(v) > 24 else "***"
    
    print(f"WB API endpoint: {WBContentCardsClient(WB_API_TOKEN).base_url}")
    print(f"WB API key (masked): {_mask(WB_API_TOKEN)}")
    print(f"Supabase URL: {SUPABASE_URL}")
    print(f"Supabase key (masked): {_mask(SUPABASE_KEY)}")
    
    # 1. Получаем карточки из WB API
    print("\n📥 Получение карточек из WB API...")
    client = WBContentCardsClient(WB_API_TOKEN)
    cards = list(client.iterate_all_cards(limit=100, with_photo=-1, locale="ru"))
    print(f"   Получено карточек: {len(cards)}")
    
    if not cards:
        print("⚠️ Пустой список cards, завершаем")
        return
    
    # 2. Валидация структуры
    print("\n🔍 Валидация структуры...")
    if not check_and_validate_structure(cards):
        print("❌ Валидация не пройдена, выполнение остановлено")
        return
    
    # 3. Нормализация данных
    print("\n🔧 Нормализация данных...")
    products_data, product_sizes_data = extract_data_for_supabase(cards)
    print(f"   Товаров (products): {len(products_data)}")
    print(f"   Размеров/баркодов (product_sizes): {len(product_sizes_data)}")
    
    # 4. Применение исключений
    if EXCLUDED_NM_IDS:
        print(f"\n🚫 Применение исключений (excluded_nm_ids)...")
        print(f"   Исключений: {len(EXCLUDED_NM_IDS)}")
        before_products = len(products_data)
        before_sizes = len(product_sizes_data)
        
        products_data = [p for p in products_data if p['nm_id'] not in EXCLUDED_NM_IDS]
        product_sizes_data = [s for s in product_sizes_data if s['nm_id'] not in EXCLUDED_NM_IDS]
        
        print(f"   Отфильтровано товаров: {before_products - len(products_data)}")
        print(f"   Отфильтровано размеров: {before_sizes - len(product_sizes_data)}")
    
    if not products_data:
        print("⚠️ После фильтрации не осталось товаров для записи")
        return
    
    # 5. Подключение к Supabase
    print("\n🔗 Подключение к Supabase...")
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("   ✅ Подключение установлено")
    except Exception as e:
        print(f"   ❌ ОШИБКА подключения: {e}")
        return
    
    # 6. Запись в Supabase
    try:
        upsert_to_supabase(supabase, products_data, product_sizes_data)
    except Exception as e:
        print(f"\n❌ ОШИБКА при записи в Supabase: {e}")
        return
    
    print("\n✅ Готово! База артикулов обновлена в Supabase")


if __name__ == "__main__":
    main()

