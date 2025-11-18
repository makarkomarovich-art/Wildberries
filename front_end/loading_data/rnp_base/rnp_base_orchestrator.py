#!/usr/bin/env python3
"""
RNP Base Orchestrator - главная функция для обновления "База РНП".

Полный цикл:
1. Валидация конфигурации
2. Fetch хедеров из Google
3. Fetch данных из Supabase (4 уровня агрегации)
4. Создание backup листа
5. Очистка основного листа
6. Запись новых данных
7. Валидация результатов
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import logging
from datetime import date

from api_keys import (
    RNP_REPORT_ID,
    GOOGLE_CREDENTIALS_INFO,
    SUPABASE_DB_URL_LOCAL as SUPABASE_DB_URL,
)

# Локальные импорты (абсолютные пути)
from front_end.loading_data.rnp_base.header_config import HEADER_MAPPING, SHEET_NAME, BACKUP_SHEET_NAME, DAYS_BACK
from front_end.loading_data.rnp_base.structure_validator import validate_header_config, validate_google_sheet_headers
from front_end.loading_data.rnp_base.rnp_base_fetcher import (
    fetch_date_range,
    fetch_raw_data_for_level_1,
    fetch_raw_data_for_aggregation,
    fetch_current_sheet_data,
    fetch_headers_from_sheet,
)
from front_end.loading_data.rnp_base.rnp_base_aggregator import (
    build_level_1_articulы,
    build_level_2_предметы,
    build_level_3_склейки,
    build_level_4_магазин,
    combine_all_levels,
)
from front_end.loading_data.rnp_base.rnp_base_transformer import transform_to_sheet_format
from front_end.loading_data.rnp_base.rnp_base_writer import (
    get_google_sheets_service,
    check_or_create_sheet,
    clear_sheet_data,
    write_headers,
    write_data_rows,
    format_date_column,
    create_backup,
)


# ============================================================================
# НАСТРОЙКИ ЛОГИРОВАНИЯ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)


def main():
    """Main entry point для обновления "База РНП"."""
    
    print("=" * 80)
    print("🚀 RNP BASE: Обновление единой базы данных (База РНП)")
    print("=" * 80)
    print()
    
    try:
        # ====================================================================
        # ШАГ 1: Валидация конфигурации
        # ====================================================================
        logging.info("📋 ШАГ 1: Валидация конфигурации")
        logging.info("-" * 80)
        
        is_valid, errors = validate_header_config()
        if not is_valid:
            logging.error("❌ Конфигурация невалидна:")
            for err in errors:
                logging.error(f"   - {err}")
            return False
        
        # Преобразуем header_mapping в список названий для Google Sheets
        header_names = [HEADER_MAPPING[col]["name"] for col in sorted(HEADER_MAPPING.keys())]
        
        print()
        
        # ====================================================================
        # ШАГ 2: Подключение к Google Sheets
        # ====================================================================
        logging.info("🔌 ШАГ 2: Подключение к Google Sheets")
        logging.info("-" * 80)
        
        if not GOOGLE_CREDENTIALS_INFO:
            logging.error("❌ GOOGLE_CREDENTIALS_INFO не найдены в api_keys.py")
            return False
        
        service = get_google_sheets_service(GOOGLE_CREDENTIALS_INFO)
        logging.info(f"✅ Подключено к таблице: {RNP_REPORT_ID}")
        
        print()
        
        # ====================================================================
        # ШАГ 3: Fetch хедеров из Google
        # ====================================================================
        logging.info("📖 ШАГ 3: Fetch хедеров из Google Sheets")
        logging.info("-" * 80)
        
        google_headers = fetch_headers_from_sheet(service, RNP_REPORT_ID, SHEET_NAME)
        
        if google_headers:
            is_valid, errors = validate_google_sheet_headers(google_headers)
            if not is_valid:
                logging.error("❌ Хедеры Google Sheets не совпадают с конфигурацией")
                logging.info("💡 Используем хедеры из конфигурации")
        
        print()
        
        # ====================================================================
        # ШАГ 4: Fetch текущих данных для backup
        # ====================================================================
        logging.info("💾 ШАГ 4: Fetch текущих данных для backup")
        logging.info("-" * 80)
        
        current_data = fetch_current_sheet_data(service, RNP_REPORT_ID, SHEET_NAME)
        logging.info(f"✅ Fetched {len(current_data)} строк (включая хедеры)")
        
        print()
        
        # ====================================================================
        # ШАГ 5: Fetch данных из Supabase (4 уровня)
        # ====================================================================
        logging.info("📊 ШАГ 5: Fetch данных из Supabase")
        logging.info("-" * 80)
        
        date_range = fetch_date_range(DAYS_BACK)
        
        # LEVEL 1: vendor_code
        raw_level_1 = fetch_raw_data_for_level_1(date_range, SUPABASE_DB_URL)
        
        # LEVEL 2: category_wb
        raw_level_2 = fetch_raw_data_for_aggregation(date_range, SUPABASE_DB_URL, "category_wb")
        
        # LEVEL 3: imt_id
        raw_level_3 = fetch_raw_data_for_aggregation(date_range, SUPABASE_DB_URL, "imt_id")
        
        # LEVEL 4: store
        raw_level_4 = fetch_raw_data_for_aggregation(date_range, SUPABASE_DB_URL, "store")
        
        print()
        
        # ====================================================================
        # ШАГ 6: Агрегация 4 уровней
        # ====================================================================
        logging.info("🔨 ШАГ 6: Агрегация данных (4 уровня)")
        logging.info("-" * 80)
        
        level_1 = build_level_1_articulы(raw_level_1)
        level_2 = build_level_2_предметы(raw_level_2)
        level_3 = build_level_3_склейки(raw_level_3)
        level_4 = build_level_4_магазин(raw_level_4)
        
        combined = combine_all_levels(level_1, level_2, level_3, level_4)
        
        print()
        
        # ====================================================================
        # ШАГ 7: Трансформация в Google Sheets формат
        # ====================================================================
        logging.info("🔄 ШАГ 7: Трансформация в Google Sheets формат")
        logging.info("-" * 80)
        
        transformed_data = transform_to_sheet_format(combined)
        logging.info(f"✅ Трансформировано {len(transformed_data)} строк")
        
        print()
        
        # ====================================================================
        # ШАГ 8: Создание backup листа
        # ====================================================================
        logging.info("🔄 ШАГ 8: Создание backup листа")
        logging.info("-" * 80)
        
        backup_success = create_backup(
            service,
            RNP_REPORT_ID,
            SHEET_NAME,
            BACKUP_SHEET_NAME,
            current_data,
            header_names
        )
        
        if not backup_success:
            logging.warning("⚠️  Backup создан с предупреждениями")
        
        print()
        
        # ====================================================================
        # ШАГ 9: Очистка основного листа
        # ====================================================================
        logging.info("🗑️  ШАГ 9: Очистка основного листа")
        logging.info("-" * 80)
        
        clear_success = clear_sheet_data(service, RNP_REPORT_ID, SHEET_NAME, start_row=2)
        
        if not clear_success:
            logging.error("❌ Ошибка при очистке листа")
            return False
        
        print()
        
        # ====================================================================
        # ШАГ 10: Запись хедеров
        # ====================================================================
        logging.info("📝 ШАГ 10: Запись хедеров")
        logging.info("-" * 80)
        
        headers_success = write_headers(service, RNP_REPORT_ID, SHEET_NAME, header_names)
        
        if not headers_success:
            logging.error("❌ Ошибка при записи хедеров")
            return False
        
        print()
        
        # ====================================================================
        # ШАГ 11: Запись данных
        # ====================================================================
        logging.info("💾 ШАГ 11: Запись данных в Google Sheets")
        logging.info("-" * 80)
        
        write_success, rows_written = write_data_rows(
            service,
            RNP_REPORT_ID,
            SHEET_NAME,
            transformed_data,
            start_row=2
        )
        
        if not write_success:
            logging.error("❌ Ошибка при записи данных")
            return False
        
        print()
        
        # ====================================================================
        # ШАГ 12: Форматирование колонки дат
        # ====================================================================
        logging.info("📅 ШАГ 12: Форматирование колонки дат (DD.MM.YYYY)")
        logging.info("-" * 80)
        
        format_success = format_date_column(service, RNP_REPORT_ID, SHEET_NAME, rows_written)
        
        if not format_success:
            logging.warning("⚠️  Форматирование дат применено с предупреждениями")
        
        print()
        
        # ====================================================================
        # ШАГ 13: Итоговый отчёт
        # ====================================================================
        logging.info("📊 ШАГ 13: Итоговый отчёт")
        logging.info("=" * 80)
        
        logging.info(f"✅ Успешно обновлена база РНП!")
        logging.info(f"   📋 Листов: 1 (\"База РНП\")")
        logging.info(f"   📝 Всего строк: {rows_written}")
        logging.info(f"   📊 Уровень 1 (Артикули): {len(level_1)} строк")
        logging.info(f"   📊 Уровень 2 (Предметы): {len(level_2)} строк")
        logging.info(f"   📊 Уровень 3 (Склейки): {len(level_3)} строк")
        logging.info(f"   📊 Уровень 4 (Магазин): {len(level_4)} строк")
        logging.info(f"   🔄 Backup создан в листе \"backup_rnp\"")
        
        print()
        logging.info("🎉 ОБНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО!")
        print("=" * 80)
        
        return True
    
    except Exception as e:
        logging.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

