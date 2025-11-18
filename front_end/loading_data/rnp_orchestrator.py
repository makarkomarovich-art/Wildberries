#!/usr/bin/env python3
"""
Оркестратор для последовательного запуска всех четырех функций РНП.
Запускает в порядке: по артикулам → по склейкам → по предметам → по магазину
"""
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)


def run_all_rnp_updates():
    """Запускает все четыре функции обновления РНП последовательно."""
    
    # 0. РНП BASE - единая база данных (4 уровня агрегации в одном листе)
    logging.info("=" * 60)
    logging.info("0/5: Запуск обновления РНП BASE (база данных)")
    logging.info("=" * 60)
    try:
        from front_end.loading_data.rnp_base.rnp_base_orchestrator import main as rnp_base_main
        rnp_base_main()
        logging.info("✅ РНП BASE завершено успешно\n")
    except Exception as e:
        logging.error(f"❌ Ошибка в РНП BASE: {e}")
        raise
    
    # 1. РНП по артикулам (основной, с 16 атрибутами и меткой времени)
    logging.info("=" * 60)
    logging.info("1/5: Запуск обновления РНП по артикулам")
    logging.info("=" * 60)
    try:
        from front_end.loading_data.rnp_by_article.update_sheet import main as rnp_article_main
        rnp_article_main()
        logging.info("✅ РНП по артикулам завершено успешно\n")
    except Exception as e:
        logging.error(f"❌ Ошибка в РНП по артикулам: {e}")
        raise
    
    # 2. РНП по склейкам (8 атрибутов, без метки времени)
    logging.info("\n" + "=" * 60)
    logging.info("2/5: Запуск обновления РНП по склейкам")
    logging.info("=" * 60)
    try:
        from front_end.loading_data.rnp_by_imt.update_sheet import main as rnp_imt_main
        rnp_imt_main()
        logging.info("✅ РНП по склейкам завершено успешно\n")
    except Exception as e:
        logging.error(f"❌ Ошибка в РНП по склейкам: {e}")
        raise
    
    # 3. РНП по предметам (8 атрибутов, без метки времени)
    logging.info("\n" + "=" * 60)
    logging.info("3/5: Запуск обновления РНП по предметам")
    logging.info("=" * 60)
    try:
        from front_end.loading_data.rnp_by_category.update_sheet import main as rnp_category_main
        rnp_category_main()
        logging.info("✅ РНП по предметам завершено успешно\n")
    except Exception as e:
        logging.error(f"❌ Ошибка в РНП по предметам: {e}")
        raise
    
    # 4. РНП по магазину (8 атрибутов, без метки времени)
    logging.info("\n" + "=" * 60)
    logging.info("4/5: Запуск обновления РНП по магазину")
    logging.info("=" * 60)
    try:
        from front_end.loading_data.rnp_by_shop.update_sheet import main as rnp_shop_main
        rnp_shop_main()
        logging.info("✅ РНП по магазину завершено успешно\n")
    except Exception as e:
        logging.error(f"❌ Ошибка в РНП по магазину: {e}")
        raise
    
    logging.info("\n" + "=" * 60)
    logging.info("🎉 ВСЕ ПЯТЬ ФУНКЦИЙ РНП УСПЕШНО ЗАВЕРШЕНЫ")
    logging.info("=" * 60)


if __name__ == "__main__":
    try:
        run_all_rnp_updates()
    except Exception as e:
        logging.error(f"Критическая ошибка при выполнении оркестратора: {e}")
        sys.exit(1)

