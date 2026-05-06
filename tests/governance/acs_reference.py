from __future__ import annotations

from pathlib import Path

from tests.governance.contract_kernel import ACS_TEST_CATEGORIES, GOVERNANCE_KERNEL_RULE


ROOT = Path(__file__).resolve().parents[2]
ACS_PATH = ROOT / "ARCHITECTURAL_CONTRACT_SPEC.md"


def read_acs_text() -> str:
    assert ACS_PATH.exists(), f"Missing authoritative governance spec: {ACS_PATH}"
    return ACS_PATH.read_text(encoding="utf-8")


def governance_kernel_rule() -> str:
    return GOVERNANCE_KERNEL_RULE
