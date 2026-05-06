from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.governance.contract_kernel import (
    ACS_TEST_CATEGORIES,
    GOVERNANCE_KERNEL_RULE,
    POLICY_DUPLICATE_PHRASES,
    POLICY_GUARD_FILES,
)


_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate, pytest.mark.integration]


ROOT = Path(__file__).resolve().parents[2]

# Governance containment applies to read-model hardening and related guard rails.
_CLASSIFIED_TEST_FILES: dict[str, frozenset[str]] = {
    "tests/test_sprint3_tasks_read_model.py": frozenset({"contract", "determinism"}),
    "tests/test_read_model_contract_parity.py": frozenset({"determinism", "integration"}),
    "tests/test_sprint5_tasks_read_model_hardening.py": frozenset({"determinism", "integration"}),
    "tests/test_read_model_boundary_stability.py": frozenset({"determinism", "integration"}),
    "tests/test_notification_guardrails.py": frozenset({"determinism", "integration"}),
    "tests/governance/test_shared_layer_scope_limits.py": frozenset({"boundary"}),
    "tests/governance/test_shared_helper_dependency_direction.py": frozenset({"boundary"}),
    "tests/governance/test_layer_classification_policy.py": frozenset({"integration"}),
    "tests/governance/test_feature_intake_contract.py": frozenset({"integration"}),
    "tests/governance/test_meta_plane_freeze_contract.py": frozenset({"integration"}),
}


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_governed_test_files_have_explicit_classification() -> None:
    present_categories: set[str] = set()

    for relative_path, categories in _CLASSIFIED_TEST_FILES.items():
        assert (ROOT / relative_path).exists(), f"Missing governed test file: {relative_path}"
        assert categories, f"No categories assigned for: {relative_path}"
        assert len(categories) <= 2, f"Too many categories for {relative_path}: {sorted(categories)}"
        assert categories <= ACS_TEST_CATEGORIES, (
            f"Invalid categories for {relative_path}: {sorted(categories - ACS_TEST_CATEGORIES)}"
        )
        present_categories.update(categories)

    assert present_categories == ACS_TEST_CATEGORIES


def test_policy_guard_suites_use_semantic_invariants_not_rule_ids() -> None:
    for relative_path in POLICY_GUARD_FILES:
        source = _read(relative_path)
        assert "assert_rule_present(" not in source, (
            f"Rule-ID assertion found in semantic governance suite {relative_path}"
        )
        assert "assert_required_sections_present(" not in source, (
            f"Section-anchor assertion found in semantic governance suite {relative_path}"
        )
        assert re.search(r"ACS-[A-Z]+-[0-9]{3}", source) is None, (
            f"Direct ACS rule identifier usage found in semantic governance suite {relative_path}"
        )


def test_policy_text_is_not_duplicated_across_guard_suites() -> None:
    for relative_path in POLICY_GUARD_FILES:
        source = _read(relative_path).lower()
        for phrase in POLICY_DUPLICATE_PHRASES:
            assert phrase not in source, (
                f"Policy phrase duplicated in guard suite {relative_path}: {phrase}"
            )


def test_governance_kernel_rule_is_declared_once_and_referenced() -> None:
    assert GOVERNANCE_KERNEL_RULE == (
        "Enforced invariants must exist in exactly one semantic location and be referenced, not redefined."
    )


def test_boundary_tests_do_not_assert_business_logic() -> None:
    forbidden_tokens = (
        "command_type",
        "client.post(",
        "task.create",
        "task_completed",
        "schedule.create",
        "schedule.cancel",
        "reminder.create",
        "reminder.trigger",
        "delivery_status",
        "summary[",
    )

    for relative_path, categories in _CLASSIFIED_TEST_FILES.items():
        if "boundary" not in categories:
            continue

        source = _read(relative_path).lower()
        for token in forbidden_tokens:
            assert token not in source, (
                f"Boundary test leaked business logic token '{token}' in {relative_path}"
            )


def test_contract_tests_do_not_assert_cache_behavior() -> None:
    forbidden_cache_tokens = (
        "_view_cache",
        "_materialized_records_cache",
        "cache_get",
        "cache_set",
        "_get_or_build_materialized_records",
        "_get_or_build_sorted_view",
        "move_to_end",
        "popitem",
        "max_entries",
    )

    for relative_path, categories in _CLASSIFIED_TEST_FILES.items():
        if "contract" not in categories:
            continue

        source = _read(relative_path).lower()
        for token in forbidden_cache_tokens:
            assert token not in source, (
                f"Contract test leaked cache behavior token '{token}' in {relative_path}"
            )
