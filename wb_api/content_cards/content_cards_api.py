"""
Client for WB Content API: /content/v2/get/cards/list

POST https://content-api.wildberries.ru/content/v2/get/cards/list
Auth: Authorization header (token with Content/Promotion category)

When executed directly: saves response to wb_api/content_cards/
When called from main: saves response to caller's directory
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import importlib.util
import requests
import inspect
from datetime import datetime


# Load API key from api_keys.py
BASE_DIR = Path(__file__).resolve().parents[2]
api_keys_path = BASE_DIR / "api_keys.py"
spec = importlib.util.spec_from_file_location("api_keys", str(api_keys_path))
api_keys_module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(api_keys_module)
API_KEY: str = api_keys_module.WB_API_TOKEN


class WBContentCardsClient:
    """Client for listing product cards via Content API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
        self.headers = {"Authorization": api_key, "Content-Type": "application/json"}

    def fetch_cards_page(
        self,
        *,
        limit: int = 100,
        with_photo: int = -1,
        locale: Optional[str] = "ru",
        cursor_updated_at: Optional[str] = None,
        cursor_nm_id: Optional[int] = None,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Fetch one page of cards. Returns raw JSON dict.

        with_photo: -1 (all), 0 (without), 1 (with)
        """
        body: Dict[str, Any] = {
            "settings": {
                "cursor": {"limit": limit},
                "filter": {"withPhoto": with_photo},
            }
        }
        if locale:
            body["settings"]["locale"] = locale
        if cursor_updated_at is not None:
            body["settings"]["cursor"]["updatedAt"] = cursor_updated_at
        if cursor_nm_id is not None:
            body["settings"]["cursor"]["nmID"] = cursor_nm_id

        resp = requests.post(self.base_url, headers=self.headers, json=body, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def iterate_all_cards(
        self,
        *,
        limit: int = 100,
        with_photo: int = -1,
        locale: Optional[str] = "ru",
        sleep_seconds: float = 0.7,
        max_pages: Optional[int] = None,
        save_response: bool = False,
        output_dir: Optional[Path] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch multiple pages using cursor until total < limit or no cards.

        Returns a flat list of cards (concatenated).
        
        Args:
            save_response: Сохранить ответ в JSON файл
            output_dir: Директория для сохранения (по умолчанию - папка вызывающего скрипта)
        """
        all_cards: List[Dict[str, Any]] = []
        updated_at: Optional[str] = None
        nm_id: Optional[int] = None
        page_count = 0

        while True:
            if max_pages is not None and page_count >= max_pages:
                break

            data = self.fetch_cards_page(
                limit=limit,
                with_photo=with_photo,
                locale=locale,
                cursor_updated_at=updated_at,
                cursor_nm_id=nm_id,
            )
            cards = data.get("cards", []) if isinstance(data, dict) else []
            total = data.get("total") if isinstance(data, dict) else None
            cursor = data.get("cursor") if isinstance(data, dict) else None

            if not isinstance(cards, list) or not cards:
                break

            all_cards.extend(cards)
            page_count += 1

            # Prepare next cursor
            if isinstance(cursor, dict):
                updated_at = cursor.get("updatedAt")
                nm_id = cursor.get("nmID")
            else:
                updated_at = None
                nm_id = None

            # Stop if total hints the end
            if isinstance(total, int) and total < limit:
                break

            time.sleep(sleep_seconds)

        # Сохранение ответа
        if save_response:
            if output_dir is None:
                # Определяем папку вызывающего скрипта
                frame = inspect.currentframe()
                try:
                    caller_frame = frame.f_back
                    while caller_frame:
                        caller_file = caller_frame.f_globals.get('__file__')
                        if caller_file and not caller_file.endswith('content_cards_api.py'):
                            output_dir = Path(caller_file).parent
                            break
                        caller_frame = caller_frame.f_back
                    else:
                        # Fallback: папка текущего скрипта
                        output_dir = Path(__file__).parent
                finally:
                    del frame
            
            output_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"content_cards_response_{timestamp}.json"
            filepath = output_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(all_cards, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Ответ сохранен: {filepath}")

        return all_cards


def _mask(value: Optional[str]) -> str:
    if not value:
        return "<empty>"
    v = str(value)
    return (v[:12] + "..." + v[-12:]) if len(v) > 24 else "***"


def _example_run() -> None:
    client = WBContentCardsClient(API_KEY)
    print(f"Endpoint: {client.base_url}")
    print(f"API key (masked): {_mask(API_KEY)}")

    try:
        cards = client.iterate_all_cards(
            limit=100, 
            with_photo=-1, 
            locale="ru",
            save_response=True
        )
        print(f"cards count: {len(cards)}")
        if cards:
            sample = cards[0]
            print("sample keys:", sorted(sample.keys()))
    except requests.HTTPError as e:
        print("HTTP error:", e)
        if e.response is not None:
            print("Response:", e.response.text[:500])
    except Exception as e:
        print("Error:", type(e).__name__, str(e)[:500])


if __name__ == "__main__":
    _example_run()

