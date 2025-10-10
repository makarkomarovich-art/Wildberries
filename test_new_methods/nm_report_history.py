"""
Получение статистики карточек товаров Wildberries по дням
(/api/v2/nm-report/detail/history) и сохранение JSON-ответа
в ту же директорию.

Пример использования:
  python nm_report_history.py --nmids 12345 23456 --begin 2025-09-27 --end 2025-10-03
Если --begin/--end не заданы, по умолчанию используется максимальный
допустимый период — последние 7 дней.

Требуется `WB_API_TOKEN` в `api_keys.py`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional

import requests

try:
    # Ensure project root is on sys.path so imports work when script is run from test_new_methods/
    from api_keys import WB_API_TOKEN  # type: ignore
except ModuleNotFoundError:
    # Try to add parent directory (project root) dynamically and re-import
    project_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(project_root))
    try:
        from api_keys import WB_API_TOKEN  # type: ignore
    except Exception as exc:  # pragma: no cover - runtime import guard
        print("Ошибка: не удалось импортировать WB_API_TOKEN из api_keys.py после добавления project root в sys.path.", file=sys.stderr)
        raise
except Exception as exc:  # pragma: no cover - runtime import guard
    print("Ошибка при импорте WB_API_TOKEN:", exc, file=sys.stderr)
    raise


BASE_URL = "https://seller-analytics-api.wildberries.ru"
PATH = "/api/v2/nm-report/detail/history"


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def build_payload(nmids: List[int], begin: date, end: date) -> dict:
    return {"nmIDs": nmids, "period": {"begin": begin.isoformat(), "end": end.isoformat()}}


def save_json(data: Any, directory: Path, *, filename: Optional[str] = None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if filename is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"nm_report_history_{ts}.json"
    path = directory / filename
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def fetch_history(nmids: List[int], begin: date, end: date, token: str, timeout: int = 60) -> Any:
    url = f"{BASE_URL}{PATH}"
    headers = {"Authorization": token, "Content-Type": "application/json"}
    payload = build_payload(nmids, begin, end)
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def default_last_week() -> tuple[date, date]:
    # last 7 days including today as end
    end = date.today()
    begin = end - timedelta(days=6)
    return begin, end


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Получить историю статистики карточек WB и сохранить JSON в эту директорию.")
    p.add_argument("--nmids", nargs="+", type=int, required=True, help="nmID(ы) WB (максимум 20).")
    p.add_argument("--begin", type=parse_date, help="Дата начала (YYYY-MM-DD)")
    p.add_argument("--end", type=parse_date, help="Дата окончания (YYYY-MM-DD)")
    p.add_argument("--output-file", default=None, help="Имя выходного файла (по умолчанию генерируется)")
    p.add_argument("--timeout", type=int, default=60, help="Таймаут запроса в секундах")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if len(args.nmids) > 20:
        print("Ошибка: максимум 20 nmID в одном запросе.", file=sys.stderr)
        return 1

    if args.begin and args.end and args.begin > args.end:
        print("Ошибка: дата начала должна быть меньше либо равна дате окончания.", file=sys.stderr)
        return 1

    if args.begin and args.end:
        begin, end = args.begin, args.end
    else:
        begin, end = default_last_week()

    # Make request
    try:
        data = fetch_history(args.nmids, begin, end, WB_API_TOKEN, timeout=args.timeout)
    except requests.RequestException as exc:
        print(f"Запрос не удался: {exc}", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    out_path = save_json(data, script_dir, filename=args.output_file)
    print(f"Сохранён JSON отчёта NM → {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


