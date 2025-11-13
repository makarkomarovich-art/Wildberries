#!/usr/bin/env python3
"""
Utility script for creating structured backups of the local Supabase database.

- Backups are stored under `backups/` in folders named `DDMMYY(ProjectName)`.
- Each backup folder contains:
    * full.sql   – full schema + data dump
    * schema.sql – schema-only dump
    * data.sql   – data-only dump (with INSERT statements)
    * meta.json  – metadata about the backup

Requirements:
- `pg_dump` must be available in PATH (ships with Supabase Docker).
- Database connection details are taken from `api_keys.py` (SUPABASE_DB_URL_LOCAL).

Usage:
    python backups/create_backup.py [--project-name "Only Wildberries"] [--output-dir backups]
"""
from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime
from pathlib import Path
import importlib.util

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_db_url() -> str:
    """Load SUPABASE_DB_URL_LOCAL from api_keys.py dynamically."""
    project_root = Path(__file__).resolve().parents[1]
    api_keys_path = project_root / "api_keys.py"
    spec = importlib.util.spec_from_file_location("api_keys", str(api_keys_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load api_keys.py for database credentials")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    db_url = getattr(module, "SUPABASE_DB_URL_LOCAL", None)
    if not db_url:
        raise RuntimeError("SUPABASE_DB_URL_LOCAL is not defined in api_keys.py")
    return db_url


def sanitize_project_name(name: str) -> str:
    """Return a filesystem-friendly project name."""
    allowed = []
    for ch in name.strip():
        if ch.isalnum():
            allowed.append(ch)
        elif ch in {" ", "-", "_"}:
            allowed.append(" ")
    sanitized = "".join(allowed).strip()
    return sanitized or "Project"


def ensure_unique_path(base_dir: Path, folder_name: str) -> Path:
    """Return a unique directory path under base_dir."""
    candidate = base_dir / folder_name
    suffix = 2
    while candidate.exists():
        candidate = base_dir / f"{folder_name}_{suffix}"
        suffix += 1
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def dump_to_file(db_url: str, output_path: Path, dump_args: list[str]) -> None:
    cmd = ["pg_dump", db_url, *dump_args]
    with output_path.open("w", encoding="utf-8") as fh:
        subprocess.run(cmd, check=True, stdout=fh)


# ---------------------------------------------------------------------------
# Main backup routine
# ---------------------------------------------------------------------------

def create_backup(project_name: str, output_dir: Path) -> Path:
    db_url = load_db_url()
    today = datetime.now()
    date_prefix = today.strftime("%d%m%y")
    project_label = sanitize_project_name(project_name).replace(" ", "")
    folder_name = f"{date_prefix}({project_label})"

    backup_root = ensure_unique_path(output_dir, folder_name)

    full_path = backup_root / "full.sql"
    schema_path = backup_root / "schema.sql"
    data_path = backup_root / "data.sql"
    meta_path = backup_root / "meta.json"

    dump_to_file(db_url, full_path, ["--format", "plain"])
    dump_to_file(db_url, schema_path, ["--schema-only"])
    dump_to_file(db_url, data_path, ["--data-only", "--inserts"])

    metadata = {
        "created_at": today.isoformat(timespec="seconds"),
        "db_url": db_url,
        "project": project_name,
        "files": {
            "full": str(full_path.resolve()),
            "schema": str(schema_path.resolve()),
            "data": str(data_path.resolve()),
        },
    }
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    return backup_root


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a structured backup of the local Supabase database")
    parser.add_argument("--project-name", default=Path.cwd().name, help="Project name to include in backup folder")
    parser.add_argument("--output-dir", default="backups", help="Relative path to backups directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    backup_dir = create_backup(args.project_name, output_dir)
    print(f"✅ Backup created: {backup_dir}")


if __name__ == "__main__":
    main()
