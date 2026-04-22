from __future__ import annotations

import os
import subprocess
import sys

import pytest


def test_full_suite_passes() -> None:
    """Run the full pytest suite from inside CI as a hard integrity gate."""
    if os.getenv("RUN_FULL_SUITE_INTEGRITY", "0") != "1":
        pytest.skip("Set RUN_FULL_SUITE_INTEGRITY=1 to execute full suite integrity gate.")

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-q",
        "-k",
        "not test_full_suite_passes",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    output = "\n".join(
        [
            "===== STDOUT =====",
            result.stdout,
            "===== STDERR =====",
            result.stderr,
        ]
    )
    assert result.returncode == 0, output
