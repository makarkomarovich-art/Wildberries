"""
Клиент для официального API Wildberries: reportDetailByPeriod
Финансовые отчеты о продажах по реализации

Документация:
- Эндпоинт: https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod
- Авторизация: WB_API_TOKEN из api_keys
- Лимит: 1 запрос в минуту
- Данные доступны с 29 января 2024 года
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# Импорт WB_API_TOKEN из api_keys.py
import sys
from pathlib import Path as _Path
import importlib.util as _importlib_util

_api_keys_path = _Path(__file__).resolve().parents[2] / "api_keys.py"
_spec = _importlib_util.spec_from_file_location("api_keys", str(_api_keys_path))
_api_keys = _importlib_util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_api_keys)
WB_API_TOKEN = _api_keys.WB_API_TOKEN


class WBReportDetailByPeriodClient:
    """Клиент для получения детализации отчетов реализации."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Args:
            api_key: API ключ для авторизации (по умолчанию из api_keys.py)
        """
        self.api_key = api_key or WB_API_TOKEN
        self.base_url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
        self.headers = {"Authorization": self.api_key}

    def fetch_report_with_pagination(
        self,
        date_from: str,
        date_to: str,
        period: str = "weekly",
        limit: int = 100000,
        timeout: int = 60
    ) -> List[Dict[str, Any]]:
        """
        Получает отчет с автоматической пагинацией.
        
        Args:
            date_from: Начальная дата отчета (RFC3339 или date)
            date_to: Конечная дата отчета (RFC3339 или date)
            period: Периодичность отчета ('weekly' или 'daily')
            limit: Максимальное количество строк в одном запросе (до 100000)
            timeout: Таймаут запроса в секундах
            
        Returns:
            List[Dict]: Список всех записей отчета
        """
        all_records = []
        rrdid = 0
        
        while True:
            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "period": period,
                "limit": limit,
                "rrdid": rrdid
            }
            
            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=timeout
                )
                
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    # Пустой массив означает конец данных
                    break
                
                all_records.extend(data)
                
                # Получаем последний rrd_id для следующего запроса
                last_record = data[-1]
                rrdid = last_record.get('rrd_id')
                
                if not rrdid:
                    break
                    
            except requests.exceptions.RequestException as e:
                # Маска API ключа
                masked_key = f"{self.api_key[:4]}...{self.api_key[-4:]}" if len(self.api_key) > 8 else "***"
                logger.error(f"❌ API Request failed (Key: {masked_key}): {e}")
                raise
        
        return all_records

    def save_report_to_file(
        self,
        data: List[Dict[str, Any]],
        output_dir: Optional[Path] = None
    ) -> Path:
        """
        Сохраняет отчет в JSON файл.
        
        Args:
            data: Данные отчета
            output_dir: Директория для сохранения (по умолчанию wb_api/week_stats)
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        if output_dir is None:
            output_dir = Path(__file__).parent
        
        # Создаем имя файла с timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reportDetailByPeriod_{timestamp}.json"
        filepath = output_dir / filename
        
        # Сохраняем данные
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Отчет сохранен: {filepath}")
        return filepath


def main():
    """Пример использования клиента."""
    client = WBReportDetailByPeriodClient()
    
    # Получаем отчет
    records = client.fetch_report_with_pagination(
        date_from="2025-10-13",
        date_to="2025-10-19",
        period="weekly"
    )
    
    # Сохраняем в файл
    client.save_report_to_file(records)


if __name__ == "__main__":
    main()
