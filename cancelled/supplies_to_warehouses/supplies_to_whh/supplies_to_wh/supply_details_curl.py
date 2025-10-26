#!/usr/bin/env python3
"""
Curl-клиент для загрузки деталей поставки из WB supplyDetails API.
Использует JSON-RPC 2.0 формат и credentials из api_keys.
"""
import os
import json
import time
import requests
import sys

# Ensure project root is on sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import API keys
import api_keys

# Config
URL = "https://seller-supply.wildberries.ru/ns/sm-supply/supply-manager/api/v1/supply/supplyDetails"

# Получаем credentials для активного пользователя
ACTIVE_USER = getattr(api_keys, "ACTIVE_USER", "NOSOV")
user_config = api_keys.USERS.get(ACTIVE_USER, api_keys.USERS["NOSOV"])

AUTHORIZEV3_TOKEN = user_config["DISCOUNTS"]["AUTHORIZEV3_TOKEN"]
COOKIES = user_config["DISCOUNTS"]["COOKIES"]
USER_AGENT = user_config["DISCOUNTS"]["USER_AGENT"]

# Rate limiting: минимальная задержка между запросами
MIN_DELAY = 0.5  # секунды

HEADERS = {
    "accept": "*/*",
    "accept-language": "ru",
    "authorizev3": AUTHORIZEV3_TOKEN,
    "content-type": "application/json",
    "origin": "https://seller.wildberries.ru",
    "priority": "u=1, i",
    "referer": "https://seller.wildberries.ru/",
    "root-version": "v1.64.0",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": USER_AGENT,
    "Cookie": COOKIES
}


def fetch_supply_details(supply_id: int) -> dict:
    """
    Загружает детали поставки по supply_id.
    
    Args:
        supply_id: ID поставки (incomeId)
        
    Returns:
        dict: JSON-RPC ответ с деталями поставки
    """
    payload = {
        "params": {
            "pageNumber": 1,
            "pageSize": 20,
            "preorderID": None,
            "search": "",
            "supplyID": supply_id
        },
        "jsonrpc": "2.0",
        "id": "json-rpc_28"
    }
    
    print(f"🔄 Запрос деталей поставки {supply_id}")
    print(f"🔑 AUTHORIZEV3_TOKEN: {AUTHORIZEV3_TOKEN[:20]}...")
    print(f"🍪 COOKIES: {COOKIES[:50]}...")
    
    try:
        resp = requests.post(URL, headers=HEADERS, json=payload, timeout=30)
        print(f"📡 Статус ответа: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"❌ Ошибка: {resp.status_code}")
            print(f"Ответ: {resp.text}")
            resp.raise_for_status()
            
        data = resp.json()
        
        # Проверяем формат JSON-RPC
        if not isinstance(data, dict):
            raise ValueError(f"Ожидался объект, получен {type(data).__name__}")
        
        if "result" not in data:
            raise ValueError("Отсутствует поле 'result' в JSON-RPC ответе")
        
        print(f"✅ Получен JSON-RPC ответ")
        
        # Проверяем наличие нужного поля
        result = data.get("result", {})
        supply = result.get("supply", {})
        delivery_storage = supply.get("deliveryAndStorage", {})
        
        if "deliveryAndStorageExpr" in delivery_storage:
            expr_value = delivery_storage["deliveryAndStorageExpr"]
            print(f"📊 deliveryAndStorageExpr: {expr_value}")
        else:
            print("⚠️  deliveryAndStorageExpr не найден в ответе")
        
        return data
        
    except requests.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")
        raise
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        raise


def fetch_multiple_supply_details(supply_ids: list[int]) -> dict[int, dict]:
    """
    Загружает детали для нескольких поставок с задержками.
    
    Args:
        supply_ids: Список ID поставок
        
    Returns:
        dict: {supply_id: response_data}
    """
    results = {}
    
    print(f"🔄 Загрузка деталей для {len(supply_ids)} поставок")
    
    for i, supply_id in enumerate(supply_ids, 1):
        print(f"\n📦 [{i}/{len(supply_ids)}] Поставка {supply_id}")
        
        try:
            data = fetch_supply_details(supply_id)
            results[supply_id] = data
            
            # Задержка между запросами (кроме последнего)
            if i < len(supply_ids):
                print(f"⏳ Задержка {MIN_DELAY} сек...")
                time.sleep(MIN_DELAY)
                
        except Exception as e:
            print(f"❌ Ошибка для поставки {supply_id}: {e}")
            results[supply_id] = None
    
    successful = sum(1 for v in results.values() if v is not None)
    print(f"\n✅ Загружено успешно: {successful}/{len(supply_ids)}")
    
    return results


def save_response_json(data: dict, supply_id: int, filename: str = None) -> str:
    """Сохраняет ответ в JSON файл"""
    if not filename:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"supply_details_{supply_id}_{timestamp}.json"
    
    out_path = os.path.join(SCRIPT_DIR, filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON сохранен: {out_path}")
    return out_path


def main():
    """Main entry point для standalone выполнения"""
    print("=" * 60)
    print("🔄 Загрузка деталей поставок из WB supplyDetails")
    print("=" * 60)
    
    try:
        # Тестируем на одном supply_id
        test_supply_id = 33604958  # Из предыдущих тестов
        
        data = fetch_supply_details(test_supply_id)
        
        if data:
            save_response_json(data, test_supply_id)
            
            # Извлекаем ключевые данные
            result = data.get("result", {})
            supply = result.get("supply", {})
            
            print(f"\n🎉 ГОТОВО! Загружены детали поставки {test_supply_id}")
            print(f"📊 Supply ID: {supply.get('supplyId', 'N/A')}")
            print(f"📊 Статус: {supply.get('statusName', 'N/A')}")
            print(f"📊 Склад: {supply.get('warehouseName', 'N/A')}")
            
            delivery_storage = supply.get("deliveryAndStorage", {})
            if delivery_storage:
                print(f"📊 deliveryAndStorageExpr: {delivery_storage.get('deliveryAndStorageExpr', 'N/A')}")
        else:
            print("⚠️  Нет данных")
            
    except KeyboardInterrupt:
        print("\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
