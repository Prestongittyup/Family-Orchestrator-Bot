from __future__ import annotations

# Single-entry execution contract.
AUTHORIZED_ENTRYPOINTS = {
    "household_os.runtime.orchestrator.HouseholdOSOrchestrator.handle_request",
    # Explicit internal jobs that still route into handle_request.
    "household_os.runtime.daily_cycle.HouseholdDailyCycle.run_morning",
    "household_os.runtime.daily_cycle.HouseholdDailyCycle.run_evening",
    "household_os.runtime.lifecycle_migration.LifecycleMigrationLayer.process_command",
}

FORBIDDEN_DIRECT_SURFACES: dict[str, tuple[str, ...]] = {
    "household_os.runtime.action_pipeline": (
        "ActionPipeline.register_proposed_action",
        "ActionPipeline.approve_actions",
        "ActionPipeline.reject_actions",
        "ActionPipeline.reject_action_timeout",
        "ActionPipeline.execute_approved_actions",
    ),
    "household_os.core.household_state_graph": (
        "HouseholdStateGraphStore.load_graph",
        "HouseholdStateGraphStore.save_graph",
        "HouseholdStateGraphStore.refresh_graph",
        "HouseholdStateGraphStore.store_response",
        "HouseholdStateGraphStore.apply_approval",
    ),
    "household_os.runtime.event_store": (
        "event_store.append",
        "EventStore.append",
        "InMemoryEventStore.append",
    ),
    "apps.api.core.state_machine": (
        "StateMachine.transition_to",
    ),
    "household_os.core.decision_engine": (
        "HouseholdOSDecisionEngine.run",
    ),
}

INTERNAL_ALLOWED_CALLERS = {
    "household_os.runtime.orchestrator",
    "household_os.security.authorization_gate",
    "household_os.runtime.state_reducer",
}

SENSITIVE_MODULE_IMPORTS = tuple(FORBIDDEN_DIRECT_SURFACES.keys())

# Modules allowed to import each sensitive module directly.
ALLOWED_IMPORTERS_BY_MODULE: dict[str, tuple[str, ...]] = {
    "household_os.runtime.action_pipeline": (
        "household_os.runtime.orchestrator",
        "household_os.runtime.daily_cycle",
        "household_os.runtime.lifecycle_migration",
        "tests",
    ),
    "household_os.core.household_state_graph": (
        "household_os.runtime.orchestrator",
        "apps.api.hpal.orchestration_adapter",
        "apps.api.assistant_runtime_router",
        "tests",
    ),
    "household_os.runtime.event_store": (
        "household_os.runtime.action_pipeline",
        "household_os.runtime.lifecycle_migration",
        "tests",
    ),
    "apps.api.core.state_machine": (
        "household_os.runtime.action_pipeline",
        "household_os.runtime.state_reducer",
        "household_os.runtime.state_firewall",
        "tests",
    ),
    "household_os.core.decision_engine": (
        "household_os.runtime.orchestrator",
        "tests",
    ),
}

OBSERVABILITY_WRAPPER_MODULE_PREFIXES = (
    "logging",
    "loguru",
    "opentelemetry",
    "ddtrace",
    "sentry_sdk",
    "apps.api.observability",
)
