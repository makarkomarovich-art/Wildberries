"""
Конфигурация хедеров для листа "База РНП".
Явное маппирование: колонка Google Sheets → атрибут БД.
"""

# ============================================================================
# ХЕДЕРЫ (единые для всех 4 уровней)
# ============================================================================

HEADER_MAPPING = {
    "A": {
        "name": "Дата",
        "source": "date",
        "type": "dimension",
        "aggregation": None,
    },
    
    "B": {
        "name": "Признак",
        "source": "признак",  # Добавляется автоматически
        "type": "dimension",
        "aggregation": None,
    },
    
    "C": {
        "name": "Имя",
        "source": "имя",  # vendor_code / category_wb / imt_id / "Магазин"
        "type": "dimension",
        "aggregation": None,
    },
    
    "D": {
        "name": "Заказы",
        "source": "orders_count",
        "from_table": "cr_daily_stats",
        "type": "metric",
        "aggregation": "SUM",  # для уровней 2, 3, 4; AS IS для 1
    },
    
    "E": {
        "name": "Сумма заказов",
        "source": "orders_sum_rub",
        "from_table": "cr_daily_stats",
        "type": "metric",
        "aggregation": "SUM",
    },
    
    "F": {
        "name": "Расход на рекламу",
        "source": "sum",
        "from_table": "adv_params",
        "type": "metric",
        "aggregation": "SUM",
    },
    
    "G": {
        "name": "ДРР",
        "source": "drr",
        "type": "metric",
        "aggregation": None,
    },
    
    "H": {
        "name": "Клики общие",
        "source": "open_card_count",
        "from_table": "cr_daily_stats",
        "type": "metric",
        "aggregation": "SUM",
    },
    
    "I": {
        "name": "В корзину",
        "source": "add_to_cart_count",
        "from_table": "cr_daily_stats",
        "type": "metric",
        "aggregation": "SUM",
    },
    
    "J": {
        "name": "Конверсия в корзину",
        "source": "add_to_cart_percent",
        "type": "metric",
        "aggregation": None,
    },
    
    "K": {
        "name": "Конверсия в заказ",
        "source": "cart_to_order_percent",
        "type": "metric",
        "aggregation": None,
    },
    
    "L": {
        "name": "CPM",
        "source": "cpm",
        "type": "metric",
        "aggregation": None,
    },
    
    "M": {
        "name": "Рекламные просмотры",
        "source": "views",
        "from_table": "adv_params",
        "type": "metric",
        "aggregation": "SUM",
    },
    
    "N": {
        "name": "Рекламные клики",
        "source": "clicks",
        "from_table": "adv_params",
        "type": "metric",
        "aggregation": "SUM",
    },
    
    "O": {
        "name": "CTR",
        "source": "ctr",
        "type": "metric",
        "aggregation": None,
    },
    
    "P": {
        "name": "CPC",
        "source": "cpc",
        "type": "metric",
        "aggregation": None,
    },
    
    "Q": {
        "name": "Цена одного заказа",
        "source": "order_price",
        "from_table": "cr_daily_stats",
        "type": "metric",
        "aggregation": "AVG",
    },
}

# ============================================================================
# ПАРАМЕТРЫ ВЫПОЛНЕНИЯ
# ============================================================================

# Период выборки данных
DAYS_BACK = 90

# Google Sheets конфигурация
SHEET_NAME = "База РНП"
BACKUP_SHEET_NAME = "backup_rnp"
HEADER_ROW = 1
DATA_START_ROW = 2

# Признаки для каждого уровня
RECOGNITION_NAMES = {
    "level_1": "Артикул",
    "level_2": "Предмет",
    "level_3": "Склейка",
    "level_4": "Магазин",
}

