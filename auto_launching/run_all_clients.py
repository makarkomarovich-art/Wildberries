#!/usr/bin/env python3
from __future__ import annotations

import subprocess
from typing import Sequence

from .utils import PROJECT_ROOT, resolve_python_executable


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

