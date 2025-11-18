#!/usr/bin/env python3
"""
Orchestrator для РНП по категориям.
Запускает процесс обновления отчета по категориям (предметам).
"""
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
project_root = Path(__file__).resolve().parents[3]
sys.path.append(str(project_root))

import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)


def main():
    """Запускает обновление РНП по категориям."""
    logging.info("=" * 60)
    logging.info("Запуск обновления РНП по категориям")
    logging.info("=" * 60)
    
    try:
        from front_end.loading_data.rnp_by_category.update_sheet import main as update_main
        update_main()
        logging.info("✅ РНП по категориям завершено успешно\n")
    except Exception as e:
        logging.error(f"❌ Ошибка в РНП по категориям: {e}")
        raise


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        sys.exit(1)

