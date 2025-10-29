#!/usr/bin/env python3
"""
API client for Seller Analytics Acceptance (paid acceptance) report.

Endpoints:
- Create task: GET /api/v1/acceptance_report?dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD
- Download    : GET /api/v1/acceptance_report/tasks/{task_id}/download

Behavior:
- Authorization header uses WB_API_TOKEN from api_keys.py
- Polling download every 10 seconds up to 10 attempts (spec)
- When executed directly (if __name__ == "__main__"), saves raw JSONs into wb_api/paid_acceptance

Dynamic configuration via env with safe defaults.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# Ensure project root on path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import api_keys
except Exception as exc:  # pragma: no cover
    raise RuntimeError("Failed to import api_keys.py for WB_API_TOKEN") from exc


# Config (dynamic, no hardcoding of base)
WB_SA_API_BASE = os.getenv(
    "WB_SA_API_BASE", "https://seller-analytics-api.wildberries.ru"
)
CREATE_PATH = "/api/v1/acceptance_report"
DOWNLOAD_PATH_TMPL = "/api/v1/acceptance_report/tasks/{task_id}/download"


def _mask_token(token: Optional[str]) -> str:
    if not token:
        return "***"
    v = str(token)
    return (v[:10] + "..." + v[-6:]) if len(v) > 20 else "***"


@dataclass(frozen=True)
class CreateTaskResult:
    task_id: str
    raw: Dict[str, Any]


def create_task(
    date_from: date,
    date_to: date,
    token: Optional[str] = None,
    *,
    timeout_seconds: int = 60,
) -> CreateTaskResult:
    """Create acceptance report task."""
    token = token or api_keys.WB_API_TOKEN
    headers = {"Authorization": token}
    params = {
        "dateFrom": date_from.isoformat(),
        "dateTo": date_to.isoformat(),
    }

    url = f"{WB_SA_API_BASE}{CREATE_PATH}"
    print(f"📡 Create task: {url}")
    print(f"🔑 Authorization: {_mask_token(token)}")
    print(f"📅 Period: {params['dateFrom']} → {params['dateTo']}")

    resp = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)
    if resp.status_code != 200:
        try:
            body = resp.text[:500]
        except Exception:
            body = ""
        raise RuntimeError(
            f"Create task failed (status={resp.status_code}). {body}"
        )
    data = resp.json()
    task_id = data.get("data", {}).get("taskId") if isinstance(data, dict) else None
    if not task_id:
        raise RuntimeError("Create task succeeded but response has no data.taskId")
    print(f"✅ Task created: taskId={task_id}")
    return CreateTaskResult(task_id=task_id, raw=data)


def download_report(
    task_id: str,
    token: Optional[str] = None,
    *,
    timeout_seconds: int = 60,
) -> List[Dict[str, Any]]:
    """
    Try to download the acceptance report for given task.
    The spec suggests array root; we validate that it's a list.
    """
    token = token or api_keys.WB_API_TOKEN
    headers = {"Authorization": token}
    path = DOWNLOAD_PATH_TMPL.format(task_id=task_id)
    url = f"{WB_SA_API_BASE}{path}"
    resp = requests.get(url, headers=headers, timeout=timeout_seconds)
    if resp.status_code != 200:
        try:
            body = resp.text[:500]
        except Exception:
            body = ""
        raise RuntimeError(
            f"Download failed (status={resp.status_code}). {body}"
        )
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError("Download returned non-array JSON (expected array root)")
    return data


def fetch_report_with_polling(
    date_from: date,
    date_to: date,
    *,
    poll_interval_seconds: float = 10.0,
    initial_delay_seconds: float = 10.0,
    max_attempts: int = 10,
    token: Optional[str] = None,
    save_dir: Optional[Path] = None,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Full flow: create task -> poll download until ready.
    Returns (report_data, task_id).
    Spec: 10 attempts, 10s between, initial wait 10s.
    """
    # 1) Create task
    create_res = create_task(date_from, date_to, token=token)

    # Optional save create-task response
    if save_dir is not None:
        save_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = save_dir / f"paid_acceptance_create_{ts}.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(create_res.raw, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved create-task response: {out_path}")

    # 2) Poll download (initial wait before the first attempt)
    if initial_delay_seconds and initial_delay_seconds > 0:
        print(f"⏱️  Waiting {initial_delay_seconds}s before first download attempt...")
        time.sleep(initial_delay_seconds)

    attempts = 0
    last_error: Optional[str] = None
    while attempts < max_attempts:
        attempts += 1
        try:
            data = download_report(create_res.task_id, token=token)
            # Optional save download json
            if save_dir is not None:
                save_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                out_path = save_dir / f"paid_acceptance_download_{ts}.json"
                with out_path.open("w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"💾 Saved download response: {out_path}")
            print(f"✅ Report ready on attempt {attempts}")
            return data, create_res.task_id
        except Exception as e:
            last_error = str(e)
            print(f"⏳ Not ready yet (attempt {attempts}/{max_attempts}): {last_error}")
            time.sleep(poll_interval_seconds)

    raise RuntimeError(
        f"Report not ready after {max_attempts} attempts. Last error: {last_error}"
    )


def _default_period_last_30_days() -> Tuple[date, date]:
    today = date.today()
    begin = today - timedelta(days=30)
    return begin, today


if __name__ == "__main__":
    """
    Standalone run: fetch last 30 days and save raw JSONs into wb_api/paid_acceptance.
    Intentionally keeps all saving logic inside this module (not in main_function).
    Includes manual override fields to set a custom period.
    """
    try:
        # Manual override (edit these two lines if you want a custom period)
        manual_begin: Optional[date] = None  # e.g., date(2025, 2, 28)
        manual_end: Optional[date] = None    # e.g., date(2025, 3, 21)

        if manual_begin and manual_end:
            begin, end = manual_begin, manual_end
        else:
            begin, end = _default_period_last_30_days()

        save_dir = SCRIPT_DIR  # wb_api/paid_acceptance
        data, task_id = fetch_report_with_polling(
            begin,
            end,
            poll_interval_seconds=10.0,
            initial_delay_seconds=10.0,
            max_attempts=10,
            token=api_keys.WB_API_TOKEN,
            save_dir=save_dir,
        )
        print(f"📦 Records received: {len(data)}; taskId={task_id}")
        # Additionally keep a combined file for convenience
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        combined = {
            "meta": {"taskId": task_id, "dateFrom": begin.isoformat(), "dateTo": end.isoformat()},
            "data": data,
        }
        out_path = save_dir / f"paid_acceptance_{ts}.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved combined file: {out_path}")
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


