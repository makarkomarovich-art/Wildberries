#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wb_api.paid_acceptance.paid_acceptance_api import fetch_report_with_polling
from excel_actions.paid_acceptance_ea.structure_validator import (
    validate_acceptance_response,
    get_validation_report,
)
from excel_actions.paid_acceptance_ea.transform import (
    aggregate_acceptance,
    per_article_logs,
)
from excel_actions.paid_acceptance_ea.supabase_writer import (
    build_products_maps,
    split_known_unknown_nmids,
    upsert_paid_acceptance,
)

from supabase import create_client, Client
import api_keys


def get_supabase_client() -> Client:
    url = api_keys.SUPABASE_URL
    key = api_keys.SUPABASE_KEY
    if not url or not key:
        raise RuntimeError("Supabase credentials not configured. Check api_keys.py")
    return create_client(url, key)


def default_period_last_30_days() -> tuple[date, date]:
    today = date.today()
    begin = today - timedelta(days=30)
    return begin, today


def main() -> None:
    print("=" * 70)
    print("📦 Paid Acceptance → Supabase")
    print("=" * 70)

    # ------------------------------------------------------------------
    # MANUAL DATE OVERRIDE — EDIT HERE IF NEEDED
    # Example:
    # manual_begin = date(2025, 2, 28)
    # manual_end = date(2025, 3, 21)
    manual_begin = date(2025, 11, 1)  # ← set to date(...) to override
    manual_end = date(2025, 11, 12)   # ← set to date(...) to override
    # ------------------------------------------------------------------

    if manual_begin and manual_end:
        begin, end = manual_begin, manual_end
    else:
        begin, end = default_period_last_30_days()

    print(f"📅 Period: {begin} → {end}")

    # Step 1: Fetch report (no file saving from here)
    try:
        raw_data, task_id = fetch_report_with_polling(
            begin,
            end,
            poll_interval_seconds=10.0,
            initial_delay_seconds=10.0,
            max_attempts=10,
            token=api_keys.WB_API_TOKEN,
            save_dir=None,  # saving handled only when running API module directly
        )
        print(f"🆔 taskId: {task_id}")
    except Exception as e:
        print(f"❌ Failed to fetch report: {e}")
        sys.exit(1)

    # Step 2: Validate
    try:
        validate_acceptance_response(raw_data)
        print(f"✅ Structure validation passed (array root, {len(raw_data)} records)")
    except Exception as e:
        report = get_validation_report(raw_data)
        print(f"❌ Validation failed: {e}")
        print(f"Details: {report}")
        sys.exit(1)

    # Step 3: Transform & aggregate
    aggregated = aggregate_acceptance(raw_data)
    if not aggregated:
        print("⚠️  No data after aggregation")
        return

    # Step 4: DB upsert
    try:
        supabase = get_supabase_client()
    except Exception as e:
        print(f"❌ Supabase connect error: {e}")
        sys.exit(1)

    nm_to_product, nm_to_vendor = build_products_maps(supabase)

    # Log per-article aggregation details
    print("\n📊 Aggregation per article (vendor - date - count - total - price_per_unit):")
    for line in per_article_logs(aggregated, nm_to_vendor):
        print("  ", line)

    known_rows, missing_nm_ids = split_known_unknown_nmids(aggregated, nm_to_product, nm_to_vendor)

    if missing_nm_ids:
        print("\n⚠️  nm_id not found in products (unique, removed from processing):")
        print("   ", ", ".join(str(x) for x in sorted(missing_nm_ids)))

    if not known_rows:
        print("⚠️  Nothing to upsert (all nm_id missing in products)")
        return

    processed, updated_details = upsert_paid_acceptance(known_rows, supabase)
    print(f"\n💾 Upsert done: processed={processed}")
    if updated_details:
        print("🔁 Updated rows (nm_id, shk_create_date, count old→new, total old→new):")
        for d in updated_details:
            print(
                f"  nm_id={d['nm_id']}, date={d['shk_create_date']}, count {d['old_count']} → {d['new_count']}, total {d['old_total']} → {d['new_total']}"
            )

    # Step 5: Final validation — compare DB vs aggregated for written range
    try:
        min_date = min(r['shk_create_date'] for r in known_rows)
        max_date = max(r['shk_create_date'] for r in known_rows)
        resp = supabase.table('paid_acceptance')\
            .select('nm_id, shk_create_date, count, total')\
            .gte('shk_create_date', min_date)\
            .lte('shk_create_date', max_date)\
            .execute()

        db_map = {(row['nm_id'], row['shk_create_date']): (row['count'], str(row['total'])) for row in resp.data}
        agg_map = {(r['nm_id'], r['shk_create_date']): (r['count'], str(r['total'])) for r in known_rows}

        mismatches = []
        for k, agg_vals in agg_map.items():
            db_vals = db_map.get(k)
            if not db_vals or db_vals != agg_vals:
                mismatches.append((k, db_vals, agg_vals))

        if mismatches:
            print("\n❌ Final DB validation failed. Mismatches:")
            for (nm, d), db_vals, agg_vals in mismatches:
                print(f"  nm_id={nm}, date={d}: DB={db_vals}, AGG={agg_vals}")
        else:
            print("\n✅ Final DB validation passed")
    except Exception as e:
        print(f"⚠️  Final validation skipped due to error: {e}")

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


