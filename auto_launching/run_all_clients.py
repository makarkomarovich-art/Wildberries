#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def resolve_python_executable() -> str:
    override = os.environ.get("ONLYWB_PYTHON")
    if override and Path(override).is_file():
        return override

    current = sys.executable
    if current and Path(current).is_file():
        return current

    fallback = PROJECT_ROOT / "venv" / "bin" / "python"
    return str(fallback)


def run_command(args: Sequence[str]) -> None:
    result = subprocess.run(list(args), cwd=str(PROJECT_ROOT), check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Command {' '.join(args)} finished with code {result.returncode}")


def run_all_sequences() -> None:
    python_executable = resolve_python_executable()

    run_command([python_executable, "-m", "auto_launching.run_kuskov"])
    run_command([python_executable, "-m", "auto_launching.run_nosov"])
    run_command([python_executable, "-m", "loading_data.rnp_report_updater"])


def main() -> None:
    run_all_sequences()


if __name__ == "__main__":
    main()

