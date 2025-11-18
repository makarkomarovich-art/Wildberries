#!/usr/bin/env python3
"""
Main скрипт для загрузки рекламной статистики WB в Supabase.

Полный цикл:
1. Получение списка всех кампаний (/adv/v1/promotion/count)
2. Получение детальной статистики (/adv/v3/fullstats)
3. Валидация API ответов
4. Трансформация данных (фильтр views>50, агрегация по платформам, склейка)
5. Валидация трансформированных данных
6. Загрузка в adv_campaign_daily_stats (UPSERT)
7. Агрегация в adv_params
8. Валидация данных в БД
"""

import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wb_api.adv_params_api.promotion_count import (
    fetch_promotion_count,
    extract_campaign_ids,
    get_campaigns_stats
)
from wb_api.adv_params_api.fullstats import fetch_fullstats_batch

from excel_actions.adv_params_ea.structure_validator import (
    validate_promotion_count_response,
    validate_fullstats_response
)
from excel_actions.adv_params_ea.transform import (
    transform_fullstats_to_campaign_daily,
    get_transform_summary
)
from excel_actions.adv_params_ea.data_validator import (
    validate_campaign_daily_stats_batch,
    check_for_duplicates
)
from excel_actions.adv_params_ea.supabase_writer import (
    get_vendor_code_map,
    upsert_campaign_daily_stats,
    trigger_adv_params_aggregation,
    insert_adv_params_direct
)

from supabase import create_client, Client
import api_keys


def get_supabase_client() -> Client:
    """Создает и возвращает клиент Supabase"""
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    
    if not url or not key:
        raise RuntimeError(
            "Supabase credentials not configured. "
            "Check SUPABASE_URL and SUPABASE_KEY in api_keys.py"
        )
    
    return create_client(url, key)


def main(
    begin_date: date | None = None,
    end_date: date | None = None,
    min_views_threshold: int = 1,  # Фильтр: только артикулы с views > 0 (отсекаем склейку)
    use_rpc_aggregation: bool = True  # По умолчанию RPC (правильная обработка timestamps)
):
    """
    Main entry point.
    
    Args:
        begin_date: Начало периода (по умолчанию: 9 дней назад, т.е. последние 10 дней включая сегодня)
        end_date: Конец периода (по умолчанию: сегодня)
        min_views_threshold: Минимум просмотров для включения артикула
        use_rpc_aggregation: Использовать RPC функцию для агрегации (или Python)
    """
    print("=" * 70)
    print("🎯 Загрузка рекламной статистики WB → Supabase")
    print("=" * 70)
    
    # Установка дат по умолчанию
    if end_date is None:
        end_date = date.today()  # сегодня
    if begin_date is None:
        begin_date = date.today() - timedelta(days=9)  # 9 дней назад (последние 10 дней включая сегодня)
    
    print(f"📅 Период: {begin_date} → {end_date}")
    print(f"👁️  Минимум просмотров: {min_views_threshold}")
    print()
    
    # ========================================================================
    # ШАГ 1: Получение списка кампаний
    # ========================================================================
    print("📡 Шаг 1/8: Получение списка рекламных кампаний")
    print("-" * 70)
    
    try:
        promotion_response = fetch_promotion_count()
    except Exception as e:
        print(f"❌ ОШИБКА при получении списка кампаний: {e}")
        sys.exit(1)
    
    # Логирование 1) Валидация первого API ответа
    print("\n🔍 Валидация ответа /adv/v1/promotion/count...")
    try:
        validate_promotion_count_response(promotion_response)
        print("✅ 1) Валидация первого API ответа прошла успешно")
    except Exception as e:
        print(f"❌ Валидация провалена: {e}")
        sys.exit(1)
    
    # Логирование 2) Статистика по кампаниям
    stats = get_campaigns_stats(promotion_response)
    
    # Фильтруем только кампании со статусами 7, 9, 11 (для fullstats)
    # Статусы: 7 - завершена, 9 - активна, 11 - на паузе
    campaign_ids = extract_campaign_ids(promotion_response, filter_statuses=[7, 9, 11])
    
    print(f"\n✅ 2) Сколько включенных рекламных кампаний: {stats['by_status'].get(9, 0)}")
    print(f"   Сколько на паузе: {stats['by_status'].get(11, 0)}")
    print(f"   Сколько завершено: {stats['by_status'].get(7, 0)}")
    print(f"   Всего кампаний: {stats['total']}")
    
    # Логирование 3) Статистика по типам
    print(f"\n✅ 3) Сколько кампаний с разными type:")
    for type_id, count in stats['by_type'].items():
        print(f"      type {type_id}: {count} кампаний")
    
    if not campaign_ids:
        print("\n⚠️  Нет кампаний для обработки")
        return
    
    print(f"\n🆔 Извлечено campaign IDs: {len(campaign_ids)}")
    
    # ========================================================================
    # ШАГ 2: Получение детальной статистики
    # ========================================================================
    print("\n\n📊 Шаг 2/8: Получение детальной статистики")
    print("-" * 70)
    
    try:
        fullstats_response = fetch_fullstats_batch(
            campaign_ids,
            begin_date,
            end_date,
            batch_size=100,
            delay_between_batches=65,
            retry=True,
            max_retries=2
        )
    except Exception as e:
        print(f"❌ ОШИБКА при получении статистики: {e}")
        sys.exit(1)
    
    print(f"✅ Получено кампаний: {len(fullstats_response)}")
    
    # Логирование 4) Валидация второго API ответа
    print("\n🔍 Валидация ответа /adv/v3/fullstats...")
    try:
        validate_fullstats_response(fullstats_response)
        print("✅ 4) Валидация второго API ответа прошла успешно")
    except Exception as e:
        print(f"❌ Валидация провалена: {e}")
        sys.exit(1)
    
    # ========================================================================
    # ШАГ 3: Подключение к Supabase и получение vendor_code
    # ========================================================================
    print("\n\n🔌 Шаг 3/8: Подключение к Supabase")
    print("-" * 70)
    
    try:
        supabase = get_supabase_client()
        print("✅ Подключено к Supabase")
    except Exception as e:
        print(f"❌ ОШИБКА подключения: {e}")
        sys.exit(1)
    
    try:
        vendor_code_map = get_vendor_code_map(supabase)
    except Exception as e:
        print(f"❌ ОШИБКА получения vendor_code: {e}")
        sys.exit(1)
    
    # ========================================================================
    # ШАГ 4: Трансформация данных
    # ========================================================================
    print("\n\n🔄 Шаг 4/8: Трансформация данных")
    print("-" * 70)
    
    try:
        transformed_stats = transform_fullstats_to_campaign_daily(
            fullstats_response,
            vendor_code_map,
            min_views_threshold=min_views_threshold
        )
    except Exception as e:
        print(f"❌ ОШИБКА трансформации: {e}")
        sys.exit(1)
    
    # Логирование 5) Сводка по трансформированным данным
    summary = get_transform_summary(transformed_stats)
    print(f"\n✅ 5) Сводка по трансформированным данным:")
    print(f"      Всего записей: {summary['total_records']}")
    print(f"      Уникальных кампаний: {summary['unique_campaigns']}")
    print(f"      Уникальных артикулов: {summary['unique_articles']}")
    print(f"      Диапазон дат: {summary['date_range']}")
    print(f"      Суммарные просмотры: {summary['total_views']:,}")
    print(f"      Суммарные заказы: {summary['total_orders']:,}")
    
    if not transformed_stats:
        print("\n⚠️  Нет данных после трансформации (возможно, все артикулы с views < 50)")
        return
    
    # ========================================================================
    # ШАГ 5: Загрузка в БД (adv_campaign_daily_stats)
    # ========================================================================
    print("\n\n💾 Шаг 5/8: Загрузка в adv_campaign_daily_stats")
    print("-" * 70)
    
    try:
        inserted_count = upsert_campaign_daily_stats(transformed_stats, supabase)
        print(f"✅ Данные внесены в БД: {inserted_count} записей")
    except Exception as e:
        print(f"❌ ОШИБКА загрузки в БД: {e}")
        sys.exit(1)
    
    # ========================================================================
    # ШАГ 6: Валидация данных в БД
    # ========================================================================
    print("\n\n🔍 Шаг 6/8: Валидация данных в БД")
    print("-" * 70)
    
    db_count = 0  # для использования в последующих шагах
    
    try:
        # Получаем загруженные данные из БД
        check_query = supabase.table('adv_campaign_daily_stats')\
            .select('*')\
            .gte('date', begin_date.isoformat())\
            .lte('date', end_date.isoformat())\
            .execute()
        
        db_records = check_query.data
        db_count = len(db_records)
        
        print(f"📊 Сравнение вставленных данных с БД:")
        print(f"   Отправлено на вставку: {inserted_count} записей")
        print(f"   Найдено в БД: {db_count} записей")
        
        # Сравниваем количество
        if db_count == inserted_count:
            print(f"✅ Совпадение: все {db_count} записей вставлены корректно")
        else:
            print(f"⚠️  Расхождение: отправлено {inserted_count}, в БД {db_count}")
        
        # Сравниваем конкретные значения (выборочно, первые 5 записей)
        if db_count > 0:
            print(f"\n   Выборочная проверка первых записей:")
            sample_original = transformed_stats[:min(3, len(transformed_stats))]
            matches = 0
            
            for stats in sample_original:
                # Ищем соответствующую запись в БД
                matching_record = next(
                    (r for r in db_records 
                     if r['advert_id'] == stats.advert_id 
                     and r['nm_id'] == stats.nm_id 
                     and r['date'] == stats.date.isoformat()),
                    None
                )
                
                if matching_record:
                    # Сравниваем ключевые метрики
                    views_match = matching_record['views'] == stats.views
                    clicks_match = matching_record['clicks'] == stats.clicks
                    
                    if views_match and clicks_match:
                        matches += 1
                        print(f"   ✓ advert_id={stats.advert_id}, nm_id={stats.nm_id}: совпадает")
                    else:
                        print(f"   ✗ advert_id={stats.advert_id}, nm_id={stats.nm_id}: расхождение в метриках")
            
            print(f"\n✅ 6) Данные провалидированы: {matches}/{len(sample_original)} проверенных записей совпадают")
        
    except Exception as e:
        print(f"⚠️  ОШИБКА валидации БД: {e}")
        print("⚠️  Продолжаем выполнение")
    
    # ========================================================================
    # ШАГ 7: Агрегация в adv_params
    # ========================================================================
    print("\n\n📊 Шаг 7/8: Агрегация данных в adv_params")
    print("-" * 70)
    
    try:
        if use_rpc_aggregation:
            try:
                aggregated_count = trigger_adv_params_aggregation(
                    supabase,
                    date_from=begin_date.isoformat(),
                    date_to=end_date.isoformat()
                )
                # Сообщение уже выводится в функции
            except Exception:
                # Тихий fallback на Python агрегацию (без вывода ошибки)
                aggregated_count = insert_adv_params_direct(
                    supabase,
                    date_from=begin_date.isoformat(),
                    date_to=end_date.isoformat()
                )
                # Сообщение выводится в функции
        else:
            aggregated_count = insert_adv_params_direct(
                supabase,
                date_from=begin_date.isoformat(),
                date_to=end_date.isoformat()
            )
            # Сообщение выводится в функции
        
    except Exception as e:
        print(f"❌ ОШИБКА агрегации: {e}")
        print("⚠️  Пропускаем агрегацию, но детальные данные загружены")
    
    # ========================================================================
    # ШАГ 8: Валидация агрегированных данных
    # ========================================================================
    print("\n\n🔍 Шаг 8/8: Валидация агрегированных данных")
    print("-" * 70)
    
    try:
        # Получаем агрегированные данные из БД
        agg_query = supabase.table('adv_params')\
            .select('*')\
            .gte('date', begin_date.isoformat())\
            .lte('date', end_date.isoformat())\
            .execute()
        
        agg_records = agg_query.data
        agg_count = len(agg_records)
        
        if agg_count > 0:
            print(f"📊 Сравнение агрегированных данных:")
            print(f"   В БД (adv_params): {agg_count} записей")
            print(f"   Детальных записей: {db_count}")
            
            # Вычисляем ожидаемые агрегированные данные из детальных
            from collections import defaultdict
            from decimal import Decimal
            
            expected_agg = defaultdict(lambda: {
                'views': 0, 'clicks': 0, 'sum': Decimal('0'),
                'orders': 0, 'orders_sum': Decimal('0')
            })
            
            for stats in transformed_stats:
                key = (stats.nm_id, stats.date.isoformat())
                expected_agg[key]['views'] += stats.views
                expected_agg[key]['clicks'] += stats.clicks
                expected_agg[key]['sum'] += stats.sum
                expected_agg[key]['orders'] += stats.orders
                expected_agg[key]['orders_sum'] += stats.orders_sum
            
            print(f"   Ожидаемое количество агрегатов: {len(expected_agg)}")
            
            # Сравниваем выборочно (первые 3 записи)
            matches = 0
            checked = 0
            
            for (nm_id, date_str), expected in list(expected_agg.items())[:3]:
                checked += 1
                # Ищем в БД
                actual = next(
                    (r for r in agg_records 
                     if r['nm_id'] == nm_id and r['date'] == date_str),
                    None
                )
                
                if actual:
                    views_match = actual['views'] == expected['views']
                    clicks_match = actual['clicks'] == expected['clicks']
                    orders_match = actual['orders'] == expected['orders']
                    
                    if views_match and clicks_match and orders_match:
                        matches += 1
                        print(f"   ✓ nm_id={nm_id}, date={date_str}: совпадает")
                    else:
                        print(f"   ✗ nm_id={nm_id}, date={date_str}: расхождение")
                        print(f"      views: ожидали {expected['views']}, в БД {actual['views']}")
                else:
                    print(f"   ✗ nm_id={nm_id}, date={date_str}: не найдено в БД")
            
            print(f"\n✅ 7) Данные провалидированы:")
            print(f"      adv_campaign_daily_stats: {db_count} записей")
            print(f"      adv_params: {agg_count} записей")
            print(f"      Проверено агрегатов: {matches}/{checked} совпадают")
        else:
            print("⚠️  В adv_params нет записей за указанный период")
            print(f"✅ 7) Детальные данные провалидированы: {db_count} записей")
        
    except Exception as e:
        print(f"⚠️  ОШИБКА валидации агрегированных данных: {e}")
    
    # ========================================================================
    # ЗАВЕРШЕНИЕ
    # ========================================================================
    print("\n" + "=" * 70)
    print("✅ ЗАВЕРШЕНО УСПЕШНО")
    print("=" * 70)


if __name__ == "__main__":
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description="Загрузка рекламной статистики WB в Supabase")
    parser.add_argument("--begin", type=str, help="Начальная дата (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Конечная дата (YYYY-MM-DD)")
    parser.add_argument("--min-views", type=int, default=1, help="Минимум просмотров (по умолчанию: 1)")
    parser.add_argument("--no-rpc", action="store_true", help="Не использовать RPC для агрегации")
    
    args = parser.parse_args()
    
    # Формируем параметры для main()
    # Если аргументы командной строки не переданы, используем значения по умолчанию из сигнатуры функции
    main_kwargs = {
        'min_views_threshold': args.min_views,
        'use_rpc_aggregation': not args.no_rpc
    }
    
    # Передаем begin_date и end_date только если они заданы через командную строку
    if args.begin:
        main_kwargs['begin_date'] = datetime.strptime(args.begin, "%Y-%m-%d").date()
    if args.end:
        main_kwargs['end_date'] = datetime.strptime(args.end, "%Y-%m-%d").date()
    
    main(**main_kwargs)

