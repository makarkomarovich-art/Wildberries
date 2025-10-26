#!/usr/bin/env python3
"""
Главный оркестратор для ETL pipeline supplies_to_warehouses.
Загружает данные из WB API, обогащает их и записывает в Supabase.
"""
import sys
import time
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Импорты компонентов
from wb_api.supplies_to_wh.incomes_api import fetch_incomes
from wb_api.supplies_to_wh.supply_details_curl import fetch_supply_details
from excel_actions.supplies_to_warehouses_ea.structure_validator import validate_incomes_structure
from excel_actions.supplies_to_warehouses_ea.supply_details_validator import validate_supply_details_structure, extract_delivery_expr
from excel_actions.supplies_to_warehouses_ea.transform import (
    filter_accepted_supplies,
    prepare_for_db,
    add_delivery_expr_to_records,
    apply_fallback_delivery_expr,
    enrich_with_existing_delivery_expr
)
from excel_actions.supplies_to_warehouses_ea.supabase_writer import (
    enrich_with_product_ids,
    get_existing_supplies,
    smart_upsert_records,
    validate_inserted_data
)

from supabase import create_client
import api_keys


def get_supabase_client() -> create_client:
    """Создает и возвращает клиент Supabase"""
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    
    if not url or not key:
        raise RuntimeError(
            "Supabase credentials not configured. "
            "Check SUPABASE_URL and SUPABASE_KEY in api_keys.py"
        )
    
    return create_client(url, key)


def main():
    """Main entry point"""
    print("=" * 60)
    print("🚀 SUPPLIES TO WAREHOUSES ETL PIPELINE")
    print("=" * 60)
    
    try:
        # 1. Подключение к Supabase
        print("\n🔌 Шаг 1: Подключение к Supabase")
        supabase = get_supabase_client()
        print(f"✅ Подключено к: {api_keys.SUPABASE_URL}")
        
        # 2. Получить существующие поставки
        print("\n📊 Шаг 2: Анализ существующих данных")
        existing = get_existing_supplies(supabase)
        print(f"📊 Существующих записей в БД: {len(existing)}")
        
        # 3. Определить date_from
        print("\n📅 Шаг 3: Определение периода загрузки")
        # 🔧 НАСТРОЙКА: измени здесь дату для загрузки всех данных
        # По умолчанию: последний месяц
        # Для всех данных: date_from = "2020-01-01T00:00:00"
        date_from = "2025-01-01T00:00:00"  # None = последний месяц
        print(f"📅 Период загрузки: {'последний месяц' if date_from is None else date_from}")
        
        # 4. Загрузить данные из API /incomes (с пагинацией)
        print("\n📡 Шаг 4: Загрузка данных из API /incomes")
        incomes = fetch_incomes(date_from)  # возвращает list[dict]
        
        if not incomes:
            print("⚠️  Нет данных для обработки")
            return
        
        # 5. Валидация структуры
        print("\n🔍 Шаг 5: Валидация структуры данных")
        validate_incomes_structure(incomes)
        
        # 6. Фильтр: только status="Принято"
        print("\n🔄 Шаг 6: Фильтрация по статусу 'Принято'")
        accepted = filter_accepted_supplies(incomes)
        
        if not accepted:
            print("⚠️  Нет принятых поставок для обработки")
            return
        
        # 7. Трансформация данных
        print("\n🔄 Шаг 7: Трансформация данных")
        records = prepare_for_db(accepted)
        
        # 8. Обогащение product_id (СНАЧАЛА!)
        print("\n🔄 Шаг 8: Обогащение product_id")
        enriched_records = enrich_with_product_ids(records, supabase)
        
        if not enriched_records:
            print("⚠️  Нет записей для загрузки в БД (все артикулы отсутствуют в products)")
            return
        
        # 9. Фильтрация записей: только измененные или новые
        print("\n🔍 Шаг 9: Фильтрация записей по изменениям")
        records_to_update = []
        records_unchanged = []
        
        for record in enriched_records:
            income_id = record['income_id']
            key = (income_id, record['nm_id'])
            
            # Новая поставка или изменилась
            if key not in existing:
                records_to_update.append(record)
            elif existing[key]['last_change_date'] != record['last_change_date']:
                records_to_update.append(record)
            else:
                records_unchanged.append(record)
        
        print(f"📊 Записей для обновления: {len(records_to_update)}")
        print(f"📊 Записей без изменений: {len(records_unchanged)}")
        
        if not records_to_update:
            print("✅ Нет изменений в данных - пропускаем обновление")
            return
        
        # 10. Определить income_id для запроса delivery_expr (ТОЛЬКО ДЛЯ ИЗМЕНЕННЫХ!)
        print("\n🎯 Шаг 10: Определение поставок для загрузки delivery_expr")
        income_ids_to_fetch = set()
        
        for record in records_to_update:
            income_id = record['income_id']
            key = (income_id, record['nm_id'])
            
            # Проверяем, есть ли уже delivery_expr в БД
            if key in existing and existing[key]['has_delivery_expr']:
                # У записи уже есть delivery_expr, не запрашиваем повторно
                continue
            else:
                # Новой записи нет delivery_expr, запрашиваем
                income_ids_to_fetch.add(income_id)
        
        print(f"📊 Поставок для загрузки delivery_expr: {len(income_ids_to_fetch)}")
        
        # 11. Загрузить deliveryAndStorageExpr
        if income_ids_to_fetch:
            print("\n📡 Шаг 11: Загрузка deliveryAndStorageExpr")
            delivery_data = {}
            
            for i, income_id in enumerate(income_ids_to_fetch, 1):
                print(f"\n📦 [{i}/{len(income_ids_to_fetch)}] Поставка {income_id}")
                
                try:
                    details = fetch_supply_details(income_id)
                    validate_supply_details_structure(details)
                    expr = extract_delivery_expr(details)
                    delivery_data[income_id] = expr
                    
                    if expr:
                        print(f"✅ delivery_expr: {expr}")
                    else:
                        print(f"⚠️  delivery_expr: null")
                        
                except Exception as e:
                    print(f"❌ Ошибка загрузки delivery_expr для {income_id}: {e}")
                    delivery_data[income_id] = None
                
                # Задержка между запросами (кроме последнего)
                if i < len(income_ids_to_fetch):
                    print(f"⏳ Задержка 0.5 сек...")
                    time.sleep(0.5)
        else:
            delivery_data = {}
            print("⚠️  Нет поставок для загрузки delivery_expr")
        
        # 12. Добавить delivery_expr к записям (ТОЛЬКО ДЛЯ ИЗМЕНЕННЫХ!)
        print("\n🔄 Шаг 12: Добавление delivery_expr к записям")
        records_with_delivery = add_delivery_expr_to_records(records_to_update, delivery_data)
        
        # 12.1. Обогатить записи существующими delivery_expr из БД
        print("\n🔄 Шаг 12.1: Обогащение существующими delivery_expr из БД")
        records_with_delivery = enrich_with_existing_delivery_expr(records_with_delivery, existing)
        
        # 12.5. Fallback для записей без delivery_expr
        print("\n🔄 Шаг 12.5: Fallback для записей без delivery_expr")
        records_with_delivery = apply_fallback_delivery_expr(records_with_delivery, supabase)
        
        # 13. Upsert в БД (ТОЛЬКО ИЗМЕНЕННЫЕ ЗАПИСИ!)
        print("\n💾 Шаг 13: Умная запись в Supabase")
        new_count, updated_count = smart_upsert_records(records_with_delivery, existing, supabase)
        
        # 14. Валидация записанных данных
        print("\n🔍 Шаг 14: Валидация записанных данных")
        validate_inserted_data(records_with_delivery, supabase)
        
        # 15. Итоги
        print("\n" + "=" * 60)
        print("🎉 ETL PIPELINE ЗАВЕРШЕН!")
        print("=" * 60)
        print(f"📊 Обработано уникальных артикулов: {len(set(r['nm_id'] for r in enriched_records))}")
        print(f"📊 Записей для обновления: {len(records_to_update)}")
        print(f"📊 Записей без изменений: {len(records_unchanged)}")
        print(f"📊 Новых поставок: {new_count}")
        print(f"📊 Обновленных поставок: {updated_count}")
        print(f"📊 Пропущено (без product_id): {len(records) - len(enriched_records)}")
        print(f"📊 Загружено delivery_expr: {len(income_ids_to_fetch)}")
        print(f"📊 Всего записей в БД: {new_count + updated_count}")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
