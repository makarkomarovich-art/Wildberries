#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Set

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wb_api.paid_storage.paid_storage_api import fetch_report_with_polling
from excel_actions.paid_storage_ea.structure_validator import (
    validate_paid_storage_response,
    get_validation_report,
)
from excel_actions.paid_storage_ea.transform import (
    aggregate_paid_storage,
    summarize_by_day,
)
from excel_actions.paid_storage_ea.supabase_writer import (
    build_products_map,
    split_known_unknown_nmids,
    upsert_paid_storage,
)

from supabase import create_client, Client
import api_keys


def get_supabase_client() -> Client:
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    if not url or not key:
        raise RuntimeError("Supabase credentials not configured. Check api_keys.py")
    return create_client(url, key)


def default_period_last_7_days() -> tuple[date, date]:
    today = date.today()
    begin = today - timedelta(days=6)
    return begin, today


def main() -> None:
    print("=" * 70)
    print("📦 Paid Storage → Supabase")
    print("=" * 70)

    # ------------------------------------------------------------------
    # MANUAL DATE OVERRIDE — EDIT HERE IF NEEDED
    # Example:
    # manual_begin = date(2025, 10, 20)
    # manual_end = date(2025, 10, 26)
    manual_begin = None  # ← set to date(...) to override
    manual_end = None    # ← set to date(...) to override
    # ------------------------------------------------------------------

    if manual_begin and manual_end:
        begin, end = manual_begin, manual_end
    else:
        begin, end = default_period_last_7_days()

    print(f"📅 Period: {begin} → {end}")

    # Step 1: Fetch report (no file saving from here)
    try:
        raw_data, task_id = fetch_report_with_polling(
            begin,
            end,
            poll_interval_seconds=2.0,
            max_attempts=30,
            token=api_keys.WB_API_TOKEN,
            save_dir=None,  # saving handled only when running API module directly
        )
    except Exception as e:
        print(f"❌ Failed to fetch report: {e}")
        sys.exit(1)

    # Step 2: Validate
    try:
        validate_paid_storage_response(raw_data)
        print("✅ Validation passed")
    except Exception as e:
        report = get_validation_report(raw_data)
        print(f"❌ Validation failed: {e}")
        print(f"Details: {report}")
        sys.exit(1)

    # Step 3: Transform & aggregate
    aggregated = aggregate_paid_storage(raw_data)
    summaries = summarize_by_day(aggregated)
    print("\n📊 Aggregation summary by day:")
    for s in summaries:
        total_val = float(s['total_warehouse_price']) if hasattr(s['total_warehouse_price'], '__float__') else s['total_warehouse_price']
        print(
            f"  {s['date']} - nmId count {s['unique_nm_count']}, total = {total_val}"
        )

    if not aggregated:
        print("⚠️  No data after aggregation")
        return

    # Step 4: DB upsert
    try:
        supabase = get_supabase_client()
    except Exception as e:
        print(f"❌ Supabase connect error: {e}")
        sys.exit(1)

    products_map = build_products_map(supabase)
    known_rows, missing_nm_ids = split_known_unknown_nmids(aggregated, products_map)

    if missing_nm_ids:
        print("\n⚠️  nm_id not found in products (unique):")
        print("   ", ", ".join(str(x) for x in sorted(missing_nm_ids)))

    if not known_rows:
        print("⚠️  Nothing to upsert (all nm_id missing in products)")
        return

    processed, updated_details = upsert_paid_storage(known_rows, supabase)
    print(f"\n💾 Upsert done: processed={processed}")
    if updated_details:
        print("🔁 Updated rows (nm_id, date, old→new):")
        for d in updated_details:
            print(
                f"  nm_id={d['nm_id']}, date={d['date']}, {d['old_price']} → {d['new_price']}"
            )

    print("\n" + "=" * 70)
    print("✅ DONE")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)


