"""Конфигурация заголовков для discounts_prices Google Sheets."""

from __future__ import annotations

from typing import Dict, Tuple

# Строка, содержащая заголовки в листе Google Sheets.
HEADER_ROW_INDEX: int = 1

# Допустимые варианты названий столбцов (alias-ы) для каждого логического поля.
DISCOUNTS_PRICES_HEADER_ALIASES: Dict[str, Tuple[str, ...]] = {
    "nmID": ("nmID", "Артикул WB"),
    "prices": ("prices", "Цена продавца"),
    "discount": ("discount", "Скидка продавца"),
    "discountedPrices": ("discountedPrices"), # не используется
    "discountOnSite": ("discountOnSite", "СПП"),
    "priceafterSPP": ("priceafterSPP"), # не используется
    "competitivePrice": ("competitivePrice", "Привлекательная цена"), # не используется
    "isCompetitivePrice": ("isCompetitivePrice", "Статус привлекательной цены"), # не используется
    "hasPromotions": ("hasPromotions", "Наличие промо"),# не используется 
}

# Порядок полей для записи / чтения.
DATA_COLUMN_KEYS: Tuple[str, ...] = (
    "prices",
    "discount",
    "discountedPrices",
    "discountOnSite",
    "priceafterSPP",
    "competitivePrice",
    "isCompetitivePrice",
    "hasPromotions",
)


