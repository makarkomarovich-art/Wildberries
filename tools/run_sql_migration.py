from __future__ import annotations

import argparse
import sys
from pathlib import Path

import importlib.util


def load_db_url() -> str:
    base_dir = Path(__file__).resolve().parents[1]
    api_keys_path = base_dir / "api_keys.py"
    spec = importlib.util.spec_from_file_location("api_keys", str(api_keys_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load api_keys.py")
    api_keys = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api_keys)  # type: ignore[attr-defined]
    db_url: str = getattr(api_keys, "SUPABASE_DB_URL_LOCAL", "")
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL_LOCAL is empty in api_keys.py")
    return db_url


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a SQL migration file against local Supabase Postgres")
    parser.add_argument("--file", required=True, help="Path to .sql file")
    args = parser.parse_args()

    sql_path = Path(args.file)
    if not sql_path.exists():
        print(f"❌ SQL file not found: {sql_path}")
        return 1

    db_url = load_db_url()

    try:
        import psycopg2  # type: ignore
    except Exception as e:
        print("❌ psycopg2 not installed. Please install psycopg2-binary.")
        print(str(e))
        return 1

    sql_text = sql_path.read_text(encoding="utf-8")

    try:
        conn = psycopg2.connect(db_url)  # type: ignore
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
        conn.close()
        print(f"✅ Migration applied: {sql_path}")
        return 0
    except Exception as e:
        print(f"❌ Migration failed: {type(e).__name__}: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())



