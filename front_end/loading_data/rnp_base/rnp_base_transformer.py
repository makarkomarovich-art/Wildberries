"""
Трансформация данных в формат Google Sheets.
Применяет header_mapping для расставления значений по колонкам.
"""

import logging
from datetime import date
from typing import List, Dict, Any
from .header_config import HEADER_MAPPING


def transform_to_sheet_format(
    combined_data: List[Dict[str, Any]]
) -> List[List[Any]]:
    """
    Трансформирует данные из словарей в список списков для Google Sheets.
    Применяет маппинг хедеров: каждый столбец знает, откуда брать значение.
    
    Args:
        combined_data: Объединённые данные из всех 4 уровней
    
    Returns:
        List[List[Any]] - готовый формат для Google Sheets
    """
    logging.info("🔄 Трансформация данных в Google Sheets формат...")
    
    sheet_rows = []
    
    for row_data in combined_data:
        sheet_row = []
        
        # Проходим по каждой колонке (A, B, C, ...)
        for col_letter in sorted(HEADER_MAPPING.keys()):
            header_config = HEADER_MAPPING[col_letter]
            
            # Определяем, откуда брать значение
            source = header_config.get("source")
            
            if source == "calculated":
                # Вычисляемое поле (в Google Sheets на формулах)
                value = None
            else:
                # Обычное значение из row_data
                value = row_data.get(source)
            
            # Форматируем значение
            if value is None:
                formatted_value = ""
            elif col_letter == 'A' and isinstance(value, date):
                # Колонка A (Дата) - форматируем как YYYY-MM-DD для Google Sheets распознания
                # Google Sheets автоматически преобразует в формат DD.MM.YYYY через форматирование
                formatted_value = value.strftime("%Y-%m-%d")
            elif isinstance(value, float):
                # Округляем до 2 знаков для денег, до 1 для процентов
                if col_letter in ['L', 'O', 'P']:  # CPM, CTR, CPC
                    formatted_value = round(value, 2) if value else 0
                else:
                    formatted_value = round(value, 2) if value else 0
            elif isinstance(value, int):
                formatted_value = value
            else:
                formatted_value = str(value)
            
            sheet_row.append(formatted_value)
        
        sheet_rows.append(sheet_row)
    
    logging.info(f"✅ Трансформировано {len(sheet_rows)} строк")
    return sheet_rows


def apply_header_mapping_to_values(
    raw_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Применяет маппинг к отдельному словарю значений.
    Полезно для проверки/логирования.
    
    Args:
        raw_dict: Словарь с сырыми значениями
    
    Returns:
        Словарь с маппированными значениями
    """
    mapped = {}
    
    for col_letter, config in HEADER_MAPPING.items():
        source = config.get("source")
        
        if source == "calculated":
            mapped[col_letter] = None
        else:
            mapped[col_letter] = raw_dict.get(source)
    
    return mapped

