"""
Клиент для неофициального Wildberries API: discounts-prices
Перехваченный с веб-страницы seller.wildberries.ru

Документация (кратко):
- Эндпоинт: https://discounts-prices.wildberries.ru/ns/dp-api/discounts-prices/suppliers/api/v1/list/goods/filter
- Авторизация: authorizev3 header (JWT токен из веб-сессии)
- Куки: wbx-validation-key, x-supplier-id-external и другие
- Метод: POST с JSON телом
- Лимит: рекомендуется 1+ секунда между запросами
"""

from __future__ import annotations

import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Импорт параметров авторизации из единого api_keys.py (корень проекта)
from pathlib import Path as _Path
import importlib.util as _importlib_util

_api_keys_path = _Path(__file__).resolve().parents[2] / "api_keys.py"
_spec = _importlib_util.spec_from_file_location("api_keys", str(_api_keys_path))
_api_keys = _importlib_util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_api_keys)
AUTHORIZEV3_TOKEN, COOKIES, USER_AGENT = (
    _api_keys.AUTHORIZEV3_TOKEN,
    _api_keys.COOKIES,
    _api_keys.USER_AGENT,
)


# Локальная настройка логгера для модуля, если он запускается отдельно
_logger = logging.getLogger(__name__)
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    try:
        _root_dir = Path(__file__).resolve().parents[2]
        _log_path = _root_dir / 'discounts_prices.log'
        _fh = logging.FileHandler(_log_path, encoding='utf-8')
        _sh = logging.StreamHandler()
        _fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        _fh.setFormatter(_fmt)
        _sh.setFormatter(_fmt)
        _logger.addHandler(_fh)
        _logger.addHandler(_sh)
    except Exception:
        # В случае ошибок с файловой системой — не блокируем выполнение
        pass

class WBDiscountsPricesClient:
    """Клиент для получения данных о товарах с фильтрацией по скидкам и ценам."""

    def __init__(
        self,
        authorizev3_token: Optional[str] = None,
        cookies: Optional[str] = None,
        user_agent: Optional[str] = None
    ):
        """
        Args:
            authorizev3_token: JWT токен из заголовка authorizev3 (по умолчанию из auth_config.py)
            cookies: Строка с куками из cURL запроса (по умолчанию из auth_config.py)
            user_agent: User-Agent заголовок (по умолчанию из auth_config.py)
        """
        # Используем параметры из auth_config.py если не переданы явно
        self.authorizev3_token = authorizev3_token or AUTHORIZEV3_TOKEN
        self.cookies = cookies or COOKIES
        self.base_url = "https://discounts-prices.wildberries.ru/ns/dp-api/discounts-prices/suppliers/api/v1/list/goods/filter"
        
        # Стандартные заголовки из перехваченного cURL
        self.headers = {
            "accept": "*/*",
            "accept-language": "ru",
            "authorizev3": self.authorizev3_token,
            "content-type": "application/json",
            "origin": "https://seller.wildberries.ru",
            "priority": "u=1, i",
            "referer": "https://seller.wildberries.ru/",
            "root-version": "v1.57.3",
            "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
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

    def fetch_goods_filtered(
        self,
        limit: int = 50,
        offset: int = 0,
        facets: Optional[List] = None,
        filter_without_price: bool = False,
        filter_with_leftovers: bool = False,
        filter_without_competitive_price: bool = False,
        sort: str = "price",
        sort_order: int = 0,
        timeout: int = 60
    ) -> Dict[str, Any]:
        """
        Выполняет запрос к API для получения отфильтрованного списка товаров.

        Args:
            limit: Количество записей на странице (по умолчанию 50)
            offset: Смещение для пагинации (по умолчанию 0)
            facets: Список фильтров (по умолчанию пустой)
            filter_without_price: Фильтр товаров без цены
            filter_with_leftovers: Фильтр товаров с остатками
            filter_without_competitive_price: Фильтр без конкурентных цен
            sort: Поле сортировки (по умолчанию "price")
            sort_order: Порядок сортировки (0 - по возрастанию, 1 - по убыванию)
            timeout: Таймаут запроса в секундах

        Returns:
            Словарь с ответом API
        """
        if facets is None:
            facets = []

        payload = {
            "limit": limit,
            "offset": offset,
            "facets": facets,
            "filterWithoutPrice": filter_without_price,
            "filterWithLeftovers": filter_with_leftovers,
            "filterWithoutCompetitivePrice": filter_without_competitive_price,
            "sort": sort,
            "sortOrder": sort_order
        }

        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                cookies=self._parse_cookies(),
                json=payload,
                timeout=timeout
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error: {e}")
            print(f"Response status code: {response.status_code}")
            print(f"Response text: {response.text}")
            raise
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            raise

    def iterate_all_goods(
        self,
        *,
        page_size: int = 50,
        sleep_seconds: float = 1.0,
        max_pages: Optional[int] = None,
        **filter_params
    ) -> List[Dict[str, Any]]:
        """
        Получает все товары с пагинацией.

        Args:
            page_size: Размер страницы
            sleep_seconds: Задержка между запросами
            max_pages: Максимальное количество страниц (для ограничения)
            **filter_params: Дополнительные параметры фильтрации

        Returns:
            Список всех товаров
        """
        all_goods: List[Dict[str, Any]] = []
        offset = 0
        page_count = 0
        consecutive_empty_pages = 0

        print(f"🚀 Загрузка товаров: страница {page_size}, задержка {sleep_seconds}с")
        if max_pages:
            print(f"🔢 Лимит страниц: {max_pages}")

        while True:
            if max_pages is not None and page_count >= max_pages:
                print(f"✅ Достигнут лимит страниц: {max_pages}")
                break

            try:
                print(f"📥 Страница {page_count + 1} (offset={offset})...")
                data = self.fetch_goods_filtered(
                    limit=page_size,
                    offset=offset,
                    **filter_params
                )
            except requests.HTTPError as e:
                if e.response.status_code == 401:
                    print("❌ Ошибка авторизации: токен истек или недействителен")
                    break
                elif e.response.status_code == 429:
                    print("⏳ Rate limit: слишком много запросов, ждем...")
                    time.sleep(sleep_seconds * 2)
                    continue
                raise

            # Извлекаем товары из ответа
            goods = data.get("data", {}).get("listGoods", []) if isinstance(data, dict) else []
            
            if not goods:
                consecutive_empty_pages += 1
                print(f"📭 Пустая страница {page_count + 1} (подряд пустых: {consecutive_empty_pages})")
                
                # Если получили 3 пустые страницы подряд, прекращаем
                if consecutive_empty_pages >= 3:
                    print("🛑 Получено 3 пустые страницы подряд, завершаем загрузку")
                    break
            else:
                consecutive_empty_pages = 0
                print(f"✅ Получено товаров: {len(goods)}")

            all_goods.extend(goods)
            page_count += 1
            offset += page_size

            # Проверяем, есть ли еще данные
            if len(goods) < page_size:
                print(f"📄 Получено меньше товаров ({len(goods)}) чем размер страницы ({page_size})")
                print("🏁 Это последняя страница, завершаем загрузку")
                break

            time.sleep(sleep_seconds)

        print(f"🎉 Загрузка завершена: {page_count} страниц, {len(all_goods)} товаров")
        return all_goods

    def save_response_to_file(self, response_data: Dict[str, Any], filename: Optional[str] = None) -> None:
        """Сохраняет ответ API в JSON файл в папку данного модуля.

        Если filename не указан, формирует имя вида
        discounts_prices_response_YYYYMMDD_HHMMSS.json.
        """
        module_dir = Path(__file__).resolve().parent
        if not filename:
            filename = f"discounts_prices_response_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_path = module_dir / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, ensure_ascii=False, indent=2)
        logging.getLogger(__name__).info(f"💾 JSON ответ сохранен: {output_path}")

    def log_spp_stats(self, goods: List[Dict[str, Any]]) -> None:
        """Логирует информацию по СПП: список товаров с СПП, min и max СПП.

        Ожидается, что поле СПП приходит в goods как discountOnSite (в процентах).
        """
        logger = logging.getLogger(__name__)
        items_with_spp = []
        for item in goods:
            try:
                nm_id = item.get('nmID')
                spp = item.get('discountOnSite')
                spp_val = float(spp) if spp is not None else 0.0
                if spp_val > 0:
                    items_with_spp.append((nm_id, spp_val))
            except Exception:
                continue

        count_with_spp = len(items_with_spp)
        if count_with_spp == 0:
            logger.info("🏷️ СПП отсутствует у всех товаров (discountOnSite <= 0)")
            return

        spp_values = [v for _, v in items_with_spp]
        min_spp = min(spp_values)
        max_spp = max(spp_values)

        logger.info(f"🏷️ Товаров со СПП > 0: {count_with_spp}")
        logger.info(f"   ▸ Минимальное СПП: {min_spp:.2f}%")
        logger.info(f"   ▸ Максимальное СПП: {max_spp:.2f}%")

        # Список товаров с СПП: nmID(СПП%)
        items_preview = ", ".join([
            f"{nm}({spp:.2f}%)" for nm, spp in items_with_spp
        ])
        logger.info(f"   ▸ У каких товаров есть СПП: {items_preview}")


def test_pagination():
    """Тест пагинации с анализом данных."""
    print("=" * 60)
    print("🧪 ТЕСТ ПАГИНАЦИИ И АНАЛИЗ ДАННЫХ")
    print("=" * 60)
    
    client = WBDiscountsPricesClient()
    
    try:
        # Получаем все товары для анализа
        print("📥 Загружаем все товары...")
        all_goods = client.iterate_all_goods(
            page_size=50,
            sleep_seconds=1.0
        )

        # Логирование по СПП
        client.log_spp_stats(all_goods)

        # Сохранение агрегированного ответа в JSON рядом с модулем
        response_data = {
            "data": {"listGoods": all_goods},
            "error": False,
            "errorText": "",
            "metadata": {
                "retrieved_at": datetime.now().isoformat(),
                "total_goods": len(all_goods),
                "api_endpoint": client.base_url if hasattr(client, 'base_url') else ""
            }
        }
        client.save_response_to_file(response_data, filename="")
        
        print("\n" + "=" * 60)
        print("📊 СТАТИСТИКА ПАГИНАЦИИ")
        print("=" * 60)
        
        # Анализируем размерность страниц
        total_goods = len(all_goods)
        page_size = 50
        total_pages = (total_goods + page_size - 1) // page_size  # Округление вверх
        
        print(f"📄 Размер страницы: {page_size} товаров")
        print(f"📊 Всего страниц: {total_pages}")
        print(f"📦 Всего товаров: {total_goods}")
        
        # Анализируем классы товаров (subject)
        subjects = {}
        brands = {}
        
        for good in all_goods:
            # Подсчитываем классы товаров
            subject = good.get('subject', 'Не указан')
            subjects[subject] = subjects.get(subject, 0) + 1
            
            # Подсчитываем бренды
            brand = good.get('brand', 'Не указан')
            brands[brand] = brands.get(brand, 0) + 1
        
        print(f"\n📂 НАЙДЕННЫЕ КЛАССЫ ТОВАРОВ ({len(subjects)}):")
        print("-" * 40)
        for subject, count in sorted(subjects.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_goods) * 100
            print(f"  • {subject}: {count} товаров ({percentage:.1f}%)")
        
        print(f"\n🏷️  БРЕНДЫ ({len(brands)}):")
        print("-" * 40)
        for brand, count in sorted(brands.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_goods) * 100
            print(f"  • {brand}: {count} товаров ({percentage:.1f}%)")
        
        # Анализируем скидки
        discounts = [good.get('discount', 0) for good in all_goods if good.get('discount') is not None]
        if discounts:
            avg_discount = sum(discounts) / len(discounts)
            min_discount = min(discounts)
            max_discount = max(discounts)
            
            print(f"\n💰 АНАЛИЗ СКИДОК:")
            print("-" * 40)
            print(f"  • Средняя скидка: {avg_discount:.1f}%")
            print(f"  • Минимальная скидка: {min_discount}%")
            print(f"  • Максимальная скидка: {max_discount}%")
            print(f"  • Товаров со скидками: {len(discounts)} из {total_goods}")
        
        print(f"\n✅ ТЕСТ УСПЕШНО ЗАВЕРШЕН!")
        print(f"📈 Эффективность: {total_goods / total_pages:.1f} товаров на страницу")
        
        return all_goods
        
    except Exception as e:
        print(f"❌ Ошибка теста: {e}")
        import traceback
        traceback.print_exc()
        return []


if __name__ == "__main__":
    # Запускаем тест пагинации при прямом запуске файла
    test_pagination()