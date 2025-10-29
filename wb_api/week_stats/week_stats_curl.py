"""
Клиент для неофициального CURL API Wildberries: seller-wb-balance
Агрегированные данные отчетов по реализации

Документация:
- Эндпоинт: https://seller-services.wildberries.ru/ns/reports/seller-wb-balance/api/v1/reports-weekly/{report_id}
- Авторизация: authorizev3 токен и cookies из api_keys
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# Импорт токенов из api_keys.py
from pathlib import Path as _Path
import importlib.util as _importlib_util

_api_keys_path = _Path(__file__).resolve().parents[2] / "api_keys.py"
_spec = _importlib_util.spec_from_file_location("api_keys", str(_api_keys_path))
_api_keys = _importlib_util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_api_keys)

AUTHORIZEV3_TOKEN = _api_keys.AUTHORIZEV3_TOKEN
COOKIES = _api_keys.COOKIES
USER_AGENT = _api_keys.USER_AGENT


class WBWeeklyReportCurlClient:
    """Клиент для получения агрегированных данных отчетов через CURL API."""

    def __init__(
        self,
        authorizev3_token: Optional[str] = None,
        cookies: Optional[str] = None,
        user_agent: Optional[str] = None
    ):
        """
        Args:
            authorizev3_token: JWT токен из authorizev3 header
            cookies: Строка с куками из cURL запроса
            user_agent: User-Agent заголовок
        """
        self.authorizev3_token = authorizev3_token or AUTHORIZEV3_TOKEN
        self.cookies = cookies or COOKIES
        self.base_url = "https://seller-services.wildberries.ru/ns/reports/seller-wb-balance/api/v1/reports-weekly"
        
        self.headers = {
            "accept": "*/*",
            "accept-language": "ru",
            "authorizev3": self.authorizev3_token,
            "content-type": "application/json",
            "origin": "https://seller.wildberries.ru",
            "user-agent": user_agent or USER_AGENT
        }
    
    def _parse_cookies(self) -> Dict[str, str]:
        """Парсит строку кук в словарь для requests."""
        cookies_dict = {}
        for cookie in self.cookies.split('; '):
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                cookies_dict[key] = value
        return cookies_dict

    def fetch_report_aggregated(
        self,
        report_id: int,
        timeout: int = 60
    ) -> Dict[str, Any]:
        """
        Получает агрегированные данные отчета по ID.
        
        Args:
            report_id: ID отчета (realizationreport_id)
            timeout: Таймаут запроса в секундах
            
        Returns:
            Dict: Агрегированные данные отчета
        """
        url = f"{self.base_url}/{report_id}"
        cookies_dict = self._parse_cookies()
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                cookies=cookies_dict,
                timeout=timeout
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Извлекаем данные
            report_data = data.get('data', {})
            return report_data if report_data else {}
                
        except requests.exceptions.RequestException as e:
            # Маска токена
            masked_token = f"{self.authorizev3_token[:8]}...{self.authorizev3_token[-8:]}" if len(self.authorizev3_token) > 20 else "***"
            logger.error(f"❌ CURL Request failed for report_id {report_id} (Token: {masked_token}): {e}")
            raise

    def save_report_to_file(
        self,
        data: Dict[str, Any],
        report_id: int,
        output_dir: Optional[Path] = None
    ) -> Path:
        """
        Сохраняет агрегированные данные в JSON файл.
        
        Args:
            data: Данные отчета
            report_id: ID отчета
            output_dir: Директория для сохранения (по умолчанию wb_api/week_stats)
            
        Returns:
            Path: Путь к сохраненному файлу
        """
        if output_dir is None:
            output_dir = Path(__file__).parent
        
        # Создаем имя файла с timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"weekly_report_{report_id}_{timestamp}.json"
        filepath = output_dir / filename
        
        # Сохраняем данные
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Отчет сохранен: {filepath}")
        return filepath


def main():
    """Пример использования клиента."""
    client = WBWeeklyReportCurlClient()
    
    # Получаем отчет
    report_data = client.fetch_report_aggregated(report_id=511225775)
    
    # Сохраняем в файл
    client.save_report_to_file(report_data, report_id=511225775)


if __name__ == "__main__":
    main()
