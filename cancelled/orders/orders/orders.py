"""
Модуль для работы с API заказов Wildberries
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Импорт параметров авторизации из api_keys.py
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

import importlib.util
_api_keys_path = PROJECT_ROOT / "api_keys.py"
_spec = importlib.util.spec_from_file_location("api_keys", str(_api_keys_path))
_api_keys = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_api_keys)
API_KEY = _api_keys.WB_API_TOKEN


logger = logging.getLogger(__name__)


class WildberriesOrdersAPI:
    """Класс для работы с API заказов Wildberries"""
    
    def __init__(self, api_key: str = None):
        """
        Args:
            api_key: API ключ Wildberries (по умолчанию из api_keys.py)
        """
        self.api_key = api_key or API_KEY
        self.base_url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        self.headers = {"Authorization": self.api_key}
        
        # Маскирование ключа для логов
        masked_key = self.api_key[:10] + "..." + self.api_key[-4:] if len(self.api_key) > 14 else "***"
        logger.info(f"🔑 Используется API ключ: {masked_key}")
    
    def fetch_orders(
        self,
        date_from: str = None,
        flag: int = 0,
        sleep_seconds: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Получает заказы из API.
        
        Args:
            date_from: Дата начала (RFC3339). По умолчанию - 30 дней назад
            flag: 0 - по lastChangeDate, 1 - по дате заказа
            sleep_seconds: Задержка между запросами (для обхода лимитов)
            
        Returns:
            List[Dict]: Список заказов
        """
        # По умолчанию - последний месяц
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        logger.info(f"📥 Запрос заказов с {date_from}")
        
        all_orders = []
        current_date = date_from
        page = 1
        
        while True:
            params = {
                "dateFrom": current_date,
                "flag": flag
            }
            
            try:
                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=params,
                    timeout=60
                )
                
                if response.status_code == 401:
                    logger.error("❌ Ошибка авторизации (401). Проверьте API ключ")
                    raise Exception("Authentication failed: 401 Unauthorized")
                
                if response.status_code == 429:
                    logger.warning("⚠️  Rate limit (429). Ожидание 60 секунд...")
                    time.sleep(60)
                    continue
                
                if response.status_code != 200:
                    logger.error(f"❌ Ошибка {response.status_code}: {response.text}")
                    raise Exception(f"API returned {response.status_code}")
                
                data = response.json()
                
                # Пустой ответ - все данные выгружены
                if not data or len(data) == 0:
                    break
                
                all_orders.extend(data)
                
                # Обновляем dateFrom для следующего запроса
                last_record = data[-1]
                current_date = last_record.get('lastChangeDate', current_date)
                
                page += 1
                time.sleep(sleep_seconds)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Ошибка при запросе: {e}")
                raise
            except Exception as e:
                logger.error(f"❌ Неожиданная ошибка: {e}")
                raise
        
        logger.info(f"✅ Получено заказов: {len(all_orders)}")
        return all_orders
    
    def save_response(self, data: List[Dict[str, Any]], suffix: str = None) -> Path:
        """
        Сохраняет ответ API в JSON файл.
        
        Args:
            data: Данные для сохранения
            suffix: Суффикс для имени файла
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        output_dir = Path(__file__).parent
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        suffix_str = f"_{suffix}" if suffix else ""
        filename = f"orders_response_{timestamp}{suffix_str}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Ответ сохранен: {filepath}")
        return filepath


def main():
    """Основная функция для тестирования API"""
    print("🚀 Запуск тестирования API заказов Wildberries")
    print("=" * 50)
    
    # Создаем экземпляр API
    api = WildberriesOrdersAPI()
    
    # Получаем данные
    try:
        orders = api.fetch_orders()
        
        print("\n📊 Результаты:")
        print("=" * 30)
        print(f"Количество заказов: {len(orders)}")
        
        # Показываем структуру первого заказа
        if orders:
            print("\nПервая запись:")
            for key, value in orders[0].items():
                print(f"  {key}: {value}")
        
        # Сохраняем ответ
        api.save_response(orders)
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    main()
