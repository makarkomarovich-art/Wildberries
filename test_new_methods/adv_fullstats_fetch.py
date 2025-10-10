"""
Fetch Wildberries Advertising full statistics (adv/v3/fullstats) and save JSON to disk.

Usage examples:
  python adv_fullstats_fetch.py --ids 22161678 28449281 --begin 2025-09-07 --end 2025-09-08
  python adv_fullstats_fetch.py --ids 28155229 --begin 2025-09-01 --end 2025-09-07 --retry --max-retries 2
  python adv_fullstats_fetch.py --all --begin 2025-09-01 --end 2025-09-07 --retry

Notes:
  - Requires valid API token exported from `api_keys.WB_API_TOKEN`.
  - Max period is 31 days (inclusive).
  - Endpoint rate limit: 1 request per minute per seller account.
  - Base URL can be overridden with env var `WB_ADV_API_BASE` (default: https://advert-api.wildberries.ru).
  - Use --all to automatically fetch all campaign IDs from WB API.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    # Project-local credentials and tokens
    from api_keys import WB_API_TOKEN  # type: ignore
except Exception as exc:  # pragma: no cover - runtime import guard
    print("Failed to import WB_API_TOKEN from api_keys.py. Ensure the file exists and exports WB_API_TOKEN.", file=sys.stderr)
    raise


WB_ADV_API_BASE = os.getenv("WB_ADV_API_BASE", "https://advert-api.wildberries.ru")
FULLSTATS_PATH = "/adv/v3/fullstats"
CAMPAIGNS_LIST_PATH = "/adv/v1/promotion/count"


@dataclass
class FullstatsRequest:
    campaign_ids: List[str]
    begin: date
    end: date


def fetch_all_campaign_ids(token: str, *, timeout_seconds: int = 30) -> List[str]:
    """Fetch all campaign IDs from WB Advertising API."""
    url = f"{WB_ADV_API_BASE}{CAMPAIGNS_LIST_PATH}"
    headers = {"Authorization": token}

    try:
        response = requests.get(url, headers=headers, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()

        # Extract campaign IDs from response
        campaign_ids = []
        if "adverts" in data:
            for advert_group in data["adverts"]:
                if "advert_list" in advert_group:
                    for campaign in advert_group["advert_list"]:
                        if "advertId" in campaign:
                            campaign_ids.append(str(campaign["advertId"]))
        if not campaign_ids:
            print(f"Warning: No campaign IDs found in response from campaigns API: {data}", file=sys.stderr)
            return []
        return campaign_ids

    except requests.RequestException as e:
        print(f"Error fetching campaigns: {e}", file=sys.stderr)
        return []


def parse_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{date_str}'. Use YYYY-MM-DD.") from exc


def validate_request(req: FullstatsRequest) -> None:
    if not req.campaign_ids:
        raise ValueError("Provide at least one campaign id via --ids.")
    if len(req.campaign_ids) > 100:
        raise ValueError("Too many ids: max 100 per request.")
    if req.begin > req.end:
        raise ValueError("begin date must be <= end date.")
    delta_days = (req.end - req.begin).days + 1
    if delta_days > 31:
        raise ValueError("Period too long: max 31 days inclusive.")


def build_params(req: FullstatsRequest) -> Dict[str, str]:
    return {
        "ids": ",".join(req.campaign_ids),
        "beginDate": req.begin.isoformat(),
        "endDate": req.end.isoformat(),
    }


def fetch_fullstats(
    req: FullstatsRequest,
    token: str,
    *,
    timeout_seconds: int = 60,
    retry: bool = False,
    max_retries: int = 1,
    backoff_seconds: int = 65,
) -> Any:
    url = f"{WB_ADV_API_BASE}{FULLSTATS_PATH}"
    headers = {"Authorization": token}
    params = build_params(req)

    attempts = 0
    while True:
        attempts += 1
        response = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)
        if response.status_code == 200:
            try:
                return response.json()
            except json.JSONDecodeError as exc:
                raise RuntimeError("API returned non-JSON response") from exc

        # Handle rate limiting and transient errors
        if not retry or attempts > (max_retries + 1):
            details = _format_error_details(response)
            raise RuntimeError(f"Request failed (status={response.status_code}). {details}")

        if response.status_code in (429, 503):
            time.sleep(backoff_seconds)
            continue

        # Non-retryable error
        details = _format_error_details(response)
        raise RuntimeError(f"Request failed (status={response.status_code}). {details}")


def _format_error_details(response: requests.Response) -> str:
    try:
        payload = response.json()
        return f"Response JSON: {json.dumps(payload, ensure_ascii=False)[:500]}"
    except Exception:
        return f"Response text: {response.text[:500]}"


def save_json(data: Any, directory: Path, *, filename: Optional[str] = None) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if filename is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"adv_fullstats_response_{ts}.json"
    path = directory / filename
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch WB advertising fullstats and save JSON.")
    parser.add_argument("--ids", nargs="+", help="Campaign IDs (1..100). Can be space-separated. If not provided, use --all to fetch all campaigns.")
    parser.add_argument("--all", action="store_true", help="Fetch all campaign IDs automatically from WB API.")
    parser.add_argument("--begin", required=True, type=parse_date, help="Begin date (YYYY-MM-DD).")
    parser.add_argument("--end", required=True, type=parse_date, help="End date (YYYY-MM-DD).")
    parser.add_argument("--output-file", default=None, help="Optional output filename. Saved to script directory by default.")
    parser.add_argument("--retry", action="store_true", help="Enable simple retry on 429/503 with backoff.")
    parser.add_argument("--max-retries", type=int, default=1, help="Max retries when --retry is used (default: 1).")
    parser.add_argument("--timeout", type=int, default=60, help="Request timeout in seconds (default: 60).")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Get campaign IDs either from --ids or --all
    if args.all:
        print("Fetching all campaign IDs from WB API...")
        campaign_ids = fetch_all_campaign_ids(WB_API_TOKEN)
        if not campaign_ids:
            print("No campaigns found or failed to fetch. Exiting.", file=sys.stderr)
            return 1
        print(f"Found {len(campaign_ids)} campaigns: {', '.join(campaign_ids[:5])}{'...' if len(campaign_ids) > 5 else ''}")
    elif args.ids:
        campaign_ids = [str(v) for v in args.ids]
    else:
        print("Error: Provide either --ids or --all flag.", file=sys.stderr)
        return 1

    req = FullstatsRequest(campaign_ids=campaign_ids, begin=args.begin, end=args.end)
    validate_request(req)

    # Split into batches of 100 if needed
    all_data = []
    for i in range(0, len(campaign_ids), 100):
        batch_ids = campaign_ids[i:i+100]
        batch_req = FullstatsRequest(campaign_ids=batch_ids, begin=args.begin, end=args.end)

        print(f"Fetching stats for campaigns {i+1}-{min(i+100, len(campaign_ids))} of {len(campaign_ids)}...")
        batch_data = fetch_fullstats(
            batch_req,
            WB_API_TOKEN,
            timeout_seconds=args.timeout,
            retry=bool(args.retry),
            max_retries=int(args.max_retries),
        )
        all_data.extend(batch_data)

    # Save in the script's directory by default
    script_dir = Path(__file__).resolve().parent
    out_path = save_json(all_data, script_dir, filename=args.output_file)
    print(f"Saved fullstats JSON → {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


