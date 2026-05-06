from __future__ import annotations

from collections.abc import Sequence


GOVERNANCE_KERNEL_RULE = (
    "Enforced invariants must exist in exactly one semantic location and be referenced, not redefined."
)

ACS_TEST_CATEGORIES = frozenset({"contract", "boundary", "determinism", "integration"})

POLICY_GUARD_FILES = (
    "tests/governance/test_shared_layer_scope_limits.py",
    "tests/governance/test_shared_helper_dependency_direction.py",
    "tests/test_read_model_boundary_stability.py",
    "tests/test_read_model_contract_parity.py",
)

POLICY_DUPLICATE_PHRASES = (
    "event log is the sole authoritative write-side source of truth",
    "shared layer is utility-only",
    "domains may depend on shared",
    "shared cannot depend on domain",
    "identical input -> identical output",
)

READ_MODEL_SHARED_MODULE = "app.api.read_model_shared"

READ_MODEL_DOMAIN_SPECS = (
    ("tasks", "get_tasks", "_sorted_tasks", "task_id"),
    ("schedule", "get_schedule", "_sorted_schedule_entries", "schedule_id"),
    ("reminders", "get_reminders", "_sorted_reminders", "reminder_id"),
    ("notifications", "get_notifications", "_sorted_notifications", "notification_id"),
)

EXPECTED_SHARED_HELPER_EXPORTS = frozenset(
    {
        "safe_int",
        "normalized_search",
        "normalized_limit",
        "normalized_offset",
        "_normalized_text",
        "normalized_filter_state",
        "projection_cache_state",
        "query_cache_state",
        "sort_records_with_tie_break",
        "paginate_records",
        "cache_get",
        "cache_set",
    }
)

EXPECTED_SHARED_IMPORT_ALIASES = frozenset(
    {
        "_shared_cache_get",
        "_shared_cache_set",
        "_shared_normalized_limit",
        "_shared_normalized_offset",
        "_shared_normalized_search",
        "_shared_paginate_records",
        "_shared_projection_cache_state",
        "_shared_query_cache_state",
        "_shared_safe_int",
        "_shared_sort_records_with_tie_break",
    }
)

READ_MODEL_ENDPOINT_MARKERS = (
    "get_command_runtime_service().get_projection",
    "_get_or_build_materialized_records(",
    "_get_or_build_sorted_view(",
    "_paginated",
)

READ_MODEL_SORTED_VIEW_PREFIX_MARKERS = (
    "_shared_query_cache_state(",
    "_filtered_records_with_summary(",
)

READ_MODEL_SORTED_VIEW_SUFFIX_MARKERS = ("_cache_set(",)

SHARED_LAYER_FORBIDDEN_UTILITY_TOKENS = (
    "apirouter",
    " query(",
    "@router.get",
    "router =",
    "get_command_runtime_service",
    "_get_or_build_materialized_records",
    "_get_or_build_sorted_view",
    "_filtered_records_with_summary",
    "_summary_from_counts",
    "_view_cache",
    "_materialized_records_cache",
    "_view_cache_max_entries",
)

SHARED_LAYER_FORBIDDEN_DOMAIN_TOKENS = (
    "task.",
    "schedule.",
    "reminder.",
    "notification.",
    "task_",
    "schedule_",
    "reminder_",
    "notification_",
)

SHARED_LAYER_FORBIDDEN_POLICY_TOKENS = (
    "rules_engine",
    "risk_engine",
    "policy_engine",
    "authorize_action",
    "approval_required",
    "decision",
)

SHARED_LAYER_FORBIDDEN_ENFORCEMENT_TOKENS = (
    "enforce",
    "enforcement",
    "validate_",
    "guardrail",
)

SHARED_LAYER_FORBIDDEN_INTERPRETATION_TOKENS = (
    "domain_interpretation",
    "semantic_interpretation",
    "governance_interpretation",
)

FEATURE_INTAKE_DECLARATION_SYMBOL = "FEATURE_INTAKE_DECLARATION"

FEATURE_INTAKE_DECLARATION_KEYS = (
    "projection_impact",
    "read_model_impact",
    "kernel_interaction",
)

FEATURE_INTAKE_ALLOWED_VALUES: dict[str, frozenset[str]] = {
    "projection_impact": frozenset({"yes", "no"}),
    "read_model_impact": frozenset({"yes", "no"}),
    "kernel_interaction": frozenset({"none", "reference", "extension"}),
}

FEATURE_INTAKE_GOVERNED_PATTERNS = (
    "tests/test_*_domain.py",
    "tests/test_*_guardrails.py",
    "tests/test_sprint*.py",
    "tests/test_read_model_*.py",
)

FEATURE_INTAKE_GOVERNED_FILES = (
    "tests/test_task_creation_command_runtime.py",
    "tests/test_sprint0_sprint1_minimal_execution.py",
    "tests/test_sprint2_task_rules_loop.py",
    "tests/test_sprint3_tasks_read_model.py",
    "tests/test_sprint4_tasks_pagination_search.py",
    "tests/test_sprint5_tasks_read_model_hardening.py",
    "tests/test_notification_domain.py",
    "tests/test_notification_guardrails.py",
    "tests/test_reminder_domain.py",
    "tests/test_reminder_guardrails.py",
    "tests/test_scheduling_domain.py",
    "tests/test_read_model_boundary_stability.py",
    "tests/test_read_model_contract_parity.py",
    "tests/replay/test_event_replay_engine.py",
)

READ_MODEL_ENDPOINT_TOKENS = (
    '"/tasks"',
    '"/schedule"',
    '"/reminders"',
    '"/notifications"',
)

READ_MODEL_DETERMINISM_TOKENS = (
    "validate_replay(",
    "sort_by=",
    "order=",
    "_projection_fingerprint(",
    "cache",
)

PROJECTION_EVIDENCE_TOKENS = (
    "get_projection(",
    "projection",
    "replay(",
)


def assert_marker_order(source: str, markers: Sequence[str]) -> None:
    cursor = -1
    for marker in markers:
        index = source.find(marker)
        assert index > cursor, f"Expected marker order violation: {marker}"
        cursor = index
