"""
Main-скрипт для получения и валидации weekly reports.

Что делает:
1) Получает данные из API reportDetailByPeriod (детальные строки)
2) Валидирует структуру данных через JSON schema
3) Агрегирует данные по realizationreport_id
4) Получает агрегированные данные из CURL API для каждого report_id
5) Валидирует структуру CURL данных через JSON schema
6) Сравнивает агрегированные значения (валидация)
7) Выводит отчет о валидации
8) Сохраняет результаты в JSON файлы

Запуск:
  python3 main_function/week_stats_mf/week_stats.py
  python3 main_function/week_stats_mf/week_stats.py --date-from 2025-10-13 --date-to 2025-10-19
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple

import importlib.util
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from api_keys import SUPABASE_URL, SUPABASE_KEY
from supabase import create_client, Client

# Отключаем логирование HTTP запросов
import logging
logging.getLogger("httpx").setLevel(logging.WARNING)

# ============================================================================
# НАСТРОЙКИ ОТЧЕТА - УСТАНОВИТЕ ДАТЫ ЗДЕСЬ
# ============================================================================
REPORT_DATE_FROM = "2025-10-13"  # Начальная дата отчета
REPORT_DATE_TO = "2025-10-19"    # Конечная дата отчета
# ============================================================================


def setup_logging():
    """Настройка системы логирования."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('week_stats.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)


def import_api_client():
    """Динамический импорт клиента API."""
    BASE_DIR = Path(__file__).resolve().parents[2]
    api_path = BASE_DIR / 'wb_api' / 'week_stats' / 'week_stats_api.py'
    
    spec = importlib.util.spec_from_file_location("week_stats_api", str(api_path))
    api_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api_module)
    
    return api_module.WBReportDetailByPeriodClient


def import_curl_client():
    """Динамический импорт клиента CURL API."""
    BASE_DIR = Path(__file__).resolve().parents[2]
    curl_path = BASE_DIR / 'wb_api' / 'week_stats' / 'week_stats_curl.py'
    
    spec = importlib.util.spec_from_file_location("week_stats_curl", str(curl_path))
    curl_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(curl_module)
    
    return curl_module.WBWeeklyReportCurlClient


def import_validators():
    """Динамический импорт валидаторов."""
    BASE_DIR = Path(__file__).resolve().parents[2]
    
    # API validator
    api_validator_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'api_validator.py'
    spec_api = importlib.util.spec_from_file_location("api_validator", str(api_validator_path))
    api_validator_module = importlib.util.module_from_spec(spec_api)
    spec_api.loader.exec_module(api_validator_module)
    
    # CURL validator
    curl_validator_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'curl_validator.py'
    spec_curl = importlib.util.spec_from_file_location("curl_validator", str(curl_validator_path))
    curl_validator_module = importlib.util.module_from_spec(spec_curl)
    spec_curl.loader.exec_module(curl_validator_module)
    
    # Aggregator
    aggregator_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'aggregator.py'
    spec_agg = importlib.util.spec_from_file_location("aggregator", str(aggregator_path))
    aggregator_module = importlib.util.module_from_spec(spec_agg)
    spec_agg.loader.exec_module(aggregator_module)
    
    # Compare values
    compare_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'compare_values.py'
    spec_comp = importlib.util.spec_from_file_location("compare_values", str(compare_path))
    compare_module = importlib.util.module_from_spec(spec_comp)
    spec_comp.loader.exec_module(compare_module)
    
    # Supabase writer
    writer_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'supabase_writer.py'
    spec_writer = importlib.util.spec_from_file_location("supabase_writer", str(writer_path))
    writer_module = importlib.util.module_from_spec(spec_writer)
    spec_writer.loader.exec_module(writer_module)
    
    # Week rows writer
    rows_writer_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'week_rows_writer.py'
    spec_rows_writer = importlib.util.spec_from_file_location("week_rows_writer", str(rows_writer_path))
    rows_writer_module = importlib.util.module_from_spec(spec_rows_writer)
    spec_rows_writer.loader.exec_module(rows_writer_module)
    
    # Week stats writer
    week_stats_writer_path = BASE_DIR / 'excel_actions' / 'week_stats_ea' / 'week_stats_writer.py'
    spec_ws_writer = importlib.util.spec_from_file_location("week_stats_writer", str(week_stats_writer_path))
    week_stats_writer_module = importlib.util.module_from_spec(spec_ws_writer)
    spec_ws_writer.loader.exec_module(week_stats_writer_module)
    
    return api_validator_module, curl_validator_module, aggregator_module, compare_module, writer_module, rows_writer_module, week_stats_writer_module





def process_weekly_reports(
    date_from: str,
    date_to: str,
    period: str = "weekly"
) -> None:
    """
    Основная функция обработки weekly reports.
    
    Args:
        date_from: Начальная дата
        date_to: Конечная дата
        period: Период ('weekly' или 'daily')
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Начало обработки отчетов за период {date_from} - {date_to}")
    
    # Импортируем клиентов и модули
    APIClient = import_api_client()
    CurlClient = import_curl_client()
    api_validator, curl_validator, aggregator, compare_values, writer, rows_writer, week_stats_writer = import_validators()
    
    api_client = APIClient()
    curl_client = CurlClient()
    
    # 1. Получаем данные из API
    records = api_client.fetch_report_with_pagination(
        date_from=date_from,
        date_to=date_to,
        period=period
    )
    
    if not records:
        logger.warning("Нет данных для обработки")
        return
    
    # 2. Валидируем весь отчет целиком
    # Проверяем структуру каждой записи
    for i, record in enumerate(records):
        # Проверяем наличие обязательных полей
        required_fields = ['realizationreport_id', 'rrd_id', 'nm_id', 'retail_amount', 'ppvz_for_pay', 'delivery_rub']
        missing_fields = [field for field in required_fields if field not in record]
        
        if missing_fields:
            logger.error(f"Ошибка валидации записи {i}: отсутствуют поля {missing_fields}")
            continue
    
    logger.info("✅ Валидация основного отчета прошла успешно")
    
    # 3. Разбиваем отчет на два по realizationreport_id
    reports_by_realizationreport_id = {}
    for record in records:
        realizationreport_id = record.get('realizationreport_id')
        if realizationreport_id not in reports_by_realizationreport_id:
            reports_by_realizationreport_id[realizationreport_id] = []
        reports_by_realizationreport_id[realizationreport_id].append(record)
    
    logger.info(f"Отчет разбит на {len(reports_by_realizationreport_id)} отчетов")
    
    # 4. Подсчитываем supplier_oper_name для каждого realizationreport_id
    for realizationreport_id, report_records in reports_by_realizationreport_id.items():
        # Считаем количество по каждому supplier_oper_name
        oper_name_counts = {}
        for record in report_records:
            oper_name = record.get('supplier_oper_name', 'Не указано')
            oper_name_counts[oper_name] = oper_name_counts.get(oper_name, 0) + 1
        
        # Выводим результаты
        logger.info(f"\nrealizationreport_id {realizationreport_id}:")
        for oper_name, count in sorted(oper_name_counts.items(), key=lambda x: -x[1]):
            logger.info(f"  {oper_name} - {count} строк")
    
    # 5. Агрегируем данные с учетом логики по supplier_oper_name
    aggregated = aggregator.aggregate_by_oper_name(reports_by_realizationreport_id)
    
    # 6. Для каждого realizationreport_id загружаем CURL и сравниваем
    all_validations_passed = True
    for realizationreport_id, api_data in aggregated.items():
        logger.info(f"\nСравнение для realizationreport_id {realizationreport_id}:")
        
        # Загружаем CURL данные
        curl_data = curl_client.fetch_report_aggregated(realizationreport_id)
        
        # Валидируем CURL
        curl_validation = curl_validator.validate_curl_data(curl_data)
        if not curl_validation['valid']:
            logger.error(f"❌ Валидация структуры CURL не прошла: {', '.join(curl_validation['errors'])}")
            all_validations_passed = False
            continue
        
        logger.info("✅ Валидация структуры CURL прошла успешно")
        
        # Сравниваем значения
        is_valid, validation_details = compare_values.compare_api_curl(api_data, curl_data)
        
        # Логируем результаты сравнения
        for field, details in validation_details['fields'].items():
            status = "✅" if details['valid'] else "❌"
            api_val = f"{details['api_value']:.2f}"
            curl_val = f"{details['curl_value']:.2f}" if details['curl_value'] is not None else "None"
            diff_val = f"{details['diff']:.2f}"
            logger.info(f"{status} {field}: API={api_val}, CURL={curl_val}, Diff={diff_val}")
        
        # Итоговый статус
        if is_valid:
            logger.info("✅ Сравнение API и CURL прошло успешно")
        else:
            logger.error("❌ Сравнение API и CURL не прошло")
            all_validations_passed = False
    
    # 7. Если все валидации прошли - записываем в БД
    if all_validations_passed:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        count = writer.upsert_week_reports(aggregated, supabase)
        logger.info(f"✅ Записано записей в week_reports: {count}")
        
        # Записываем детальные строки в week_rows
        for realizationreport_id, api_data in aggregated.items():
            # Проверяем наличие отчета в week_reports
            response = supabase.table('week_reports').select('realizationreport_id').eq('realizationreport_id', realizationreport_id).execute()
            
            if not response.data:
                logger.warning(f"⚠️  Отчет {realizationreport_id} не найден в week_reports, пропускаем")
                continue
            
            logger.info(f"✅ Отчет {realizationreport_id} найден в week_reports")
            
            # Получаем детальные записи для этого отчета
            report_records = reports_by_realizationreport_id[realizationreport_id]
            
            # Вставляем строки
            rows_count, missing_nm_ids = rows_writer.insert_week_rows(
                report_records,
                supabase,
                realizationreport_id
            )
            
            # Логируем отсутствующие nm_id только если они есть
            if missing_nm_ids:
                logger.warning(f"⚠️  Для отчета {realizationreport_id} отсутствуют в products ({len(missing_nm_ids)} nm_id): {missing_nm_ids}")
            
            logger.info(f"✅ Записано строк в week_rows для отчета {realizationreport_id}: {rows_count}")
            
            # Агрегируем и записываем week_stats по nm_id
            inserted_count, total_unique_nm = week_stats_writer.insert_week_stats_for_report(
                supabase,
                realizationreport_id
            )
            logger.info(f"📦 week_stats {realizationreport_id}: обработано уникальных nm_id: {total_unique_nm}")
            logger.info(f"🆕 week_stats {realizationreport_id}: добавлено уникальных пар (report_id, nm_id): {inserted_count}")
            
            # Проверка консистентности сумм
            week_stats_writer.verify_week_stats_totals(supabase, realizationreport_id)
    else:
        logger.error("❌ Остановка выполнения: валидация не прошла")


def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description='Обработка weekly reports')
    parser.add_argument(
        '--date-from',
        type=str,
        default=REPORT_DATE_FROM,
        help='Начальная дата (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--date-to',
        type=str,
        default=REPORT_DATE_TO,
        help='Конечная дата (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--period',
        type=str,
        default='weekly',
        choices=['weekly', 'daily'],
        help='Период отчета'
    )
    
    args = parser.parse_args()
    
    # Настройка логирования
    setup_logging()
    
    # Обработка отчетов
    process_weekly_reports(
        date_from=args.date_from,
        date_to=args.date_to,
        period=args.period
    )


if __name__ == "__main__":
    main()
