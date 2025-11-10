#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable


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


MODULES_TO_RUN: tuple[str, ...] = (
    "main_function.adv_params_mf.adv_params_supabase",
    "main_function.cr_daily_stats_mf.cr_daily_stats_supabase",
    "main_function.discounts_prices_mf.discounts_prices",
    "main_function.warehouse_remains_mf.warehouse_remains",
)


def run_module(module: str, env: dict[str, str], python_executable: str) -> None:
    result = subprocess.run(
        [python_executable, "-m", module],
        cwd=str(PROJECT_ROOT),
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Module {module} finished with non-zero exit code {result.returncode}")


def run_modules_sequence(modules: Iterable[str], active_user: str) -> None:
    env = os.environ.copy()
    env["ACTIVE_USER"] = active_user
    python_executable = resolve_python_executable()

    for module in modules:
        run_module(module, env, python_executable)


def main() -> None:
    run_modules_sequence(MODULES_TO_RUN, active_user="NOSOV")


if __name__ == "__main__":
    main()

