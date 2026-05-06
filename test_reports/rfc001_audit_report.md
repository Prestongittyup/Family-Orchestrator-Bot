### A. Summary table | Category | Count | Description |

| Category | Count | Description |
|---|---:|---|
| KEEP | 30 | Retain as-is in RFC-001 CI because test directly protects gateway/event-log/SSE/policy invariants. |
| MODIFY | 74 | Coverage is useful but assertions/imports/entrypoints must be ported to RFC-001 canonical surfaces. |
| DELETE | 12 | Targets superseded pre-RFC components; coverage should be removed after preserving equivalent invariants elsewhere. |
| RISK | 8 | Valuable but too slow/flaky/recursive for commit-time gating; quarantine to scheduled reliability stages. |

### B. File-level classification

| File | Category | Reason | Suggested action |
|---|---|---|---|
| [tests/integration/architecture/test_dependency_graph.py](tests/integration/architecture/test_dependency_graph.py) | KEEP | Directly enforces dependency boundaries aligned to gateway-centric architecture. | Keep as architecture guard in CI. |
| [tests/integration/architecture/test_fetch_boundary.py](tests/integration/architecture/test_fetch_boundary.py) | KEEP | Validates fetch boundary placement and prevents bypass around orchestrated state construction. | Keep; update module references as needed. |
| [tests/integration/architecture/test_single_fetch_source.py](tests/integration/architecture/test_single_fetch_source.py) | KEEP | Ensures a single canonical fetch source to avoid split-brain state reads. | Keep as invariant guard. |
| [tests/integration/test_actor_context_propagation.py](tests/integration/test_actor_context_propagation.py) | KEEP | Verifies actor context propagation into event metadata and reducer behavior. | Keep; this protects gateway/auth provenance semantics. |
| [tests/integration/test_boundary_enforcement.py](tests/integration/test_boundary_enforcement.py) | KEEP | Confirms lifecycle boundary parsing/rejection logic for invalid states. | Keep as boundary contract test. |
| [tests/integration/test_event_replay_integrity.py](tests/integration/test_event_replay_integrity.py) | KEEP | Checks replay determinism and rejection of legacy/non-canonical lifecycle values. | Keep as event-log truth invariant. |
| [tests/integration/test_intelligence_analytics_dls.py](tests/integration/test_intelligence_analytics_dls.py) | KEEP | Validates deterministic comparator/analytics behavior where LLM is treated as measured advisory signal. | Keep; ensure no execution authority assumptions. |
| [tests/integration/test_intelligence_routing_resilience.py](tests/integration/test_intelligence_routing_resilience.py) | KEEP | Asserts degraded routing to RULE_ONLY on usage/infra failures, matching non-authoritative AI policy. | Keep as risk-gating resilience test. |
| [tests/integration/test_lifecycle_surface_consistency_guard.py](tests/integration/test_lifecycle_surface_consistency_guard.py) | KEEP | Guards lifecycle-state consistency across boundaries. | Keep as state-machine invariant. |
| [tests/integration/test_no_legacy_lifecycle_strings.py](tests/integration/test_no_legacy_lifecycle_strings.py) | KEEP | Prevents reintroduction of legacy lifecycle literals into canonical state flow. | Keep as migration guard. |
| [tests/integration/test_no_raw_lifecycle_strings.py](tests/integration/test_no_raw_lifecycle_strings.py) | KEEP | Enforces canonical lifecycle representation and parsing boundaries. | Keep as schema/invariant guard. |
| [tests/integration/test_persistence_roundtrip_integrity.py](tests/integration/test_persistence_roundtrip_integrity.py) | KEEP | Round-trip lifecycle persistence integrity remains essential under event-sourced state. | Keep but run via authorized store/orchestrator path only. |
| [tests/integration/test_policy_engine.py](tests/integration/test_policy_engine.py) | KEEP | Validates deterministic policy/rules behavior and non-execution guarantees. | Keep as rules-engine authority coverage. |
| [tests/system/test_architecture_suite.py](tests/system/test_architecture_suite.py) | KEEP | System-level architecture conformance suite for RFC boundaries. | Keep as mandatory gate. |
| [tests/system/test_event_sourcing.py](tests/system/test_event_sourcing.py) | KEEP | Core event-sourcing/state-reducer correctness coverage. | Keep as mandatory gate. |
| [tests/system/test_event_store_invariants.py](tests/system/test_event_store_invariants.py) | KEEP | Asserts event store integrity and provenance constraints. | Keep as mandatory gate. |
| [tests/test_action_event_contracts.py](tests/test_action_event_contracts.py) | KEEP | Checks action/event contract invariants needed for gateway-to-event-log consistency. | Keep in core CI. |
| [tests/test_fsm_immutability_enforcement.py](tests/test_fsm_immutability_enforcement.py) | KEEP | Ensures state transitions are governed by canonical FSM authority. | Keep in core CI. |
| [tests/test_governance_gates.py](tests/test_governance_gates.py) | KEEP | Repository governance and gate integrity checks protect drift. | Keep as release guard. |
| [tests/test_hard_freeze_regression.py](tests/test_hard_freeze_regression.py) | KEEP | Prevents prohibited runtime artifacts and architecture drift regressions. | Keep as hygiene guard. |
| [tests/test_invariance_enforcement.py](tests/test_invariance_enforcement.py) | KEEP | Protects canonical pipeline and SSE transport invariants. | Keep as core invariant suite. |
| [tests/test_layer_redundancy_guard.py](tests/test_layer_redundancy_guard.py) | KEEP | Detects duplicated/redundant architecture surfaces. | Keep as architecture anti-drift guard. |
| [tests/test_migration_cleanliness.py](tests/test_migration_cleanliness.py) | KEEP | Validates persisted lifecycle data is canonical post-migration. | Keep in migration/core gates. |
| [tests/test_no_direct_broadcaster_bypass.py](tests/test_no_direct_broadcaster_bypass.py) | KEEP | Enforces single realtime emission path via canonical broadcaster boundaries. | Keep as SSE boundary guard. |
| [tests/test_sse_event_closure.py](tests/test_sse_event_closure.py) | KEEP | Covers SSE schema parity, replay determinism, watermark ordering, and idempotency behavior. | Keep as critical realtime invariant suite. |
| [tests/test_static_silent_mutations.py](tests/test_static_silent_mutations.py) | KEEP | Detects silent mutation paths that bypass event-sourced controls. | Keep as integrity guard. |
| [tests/test_trust_surface_closure.py](tests/test_trust_surface_closure.py) | KEEP | Validates trust-surface closure and blocks direct mutation surfaces outside orchestrator path. | Keep as gateway boundary guard. |
| [tests/test_trust_surface_final_closure.py](tests/test_trust_surface_final_closure.py) | KEEP | Runtime trust-surface enforcement confirmation for sensitive components. | Keep; monitor for runtime flake but preserve in system stage. |
| [tests/test_ui_canonical_wiring_guard.py](tests/test_ui_canonical_wiring_guard.py) | KEEP | Ensures UI wiring hits canonical backend contracts rather than legacy endpoints. | Keep as boundary contract guard. |
| [tests/unit/test_lifecycle_contract_boundary.py](tests/unit/test_lifecycle_contract_boundary.py) | KEEP | Fast unit checks for lifecycle contract boundaries and mapper behavior. | Keep in default fast CI tier. |
| [tests/integration/test_brief_evaluation.py](tests/integration/test_brief_evaluation.py) | MODIFY | Touches intelligence/planning behavior that must be explicitly non-authoritative for execution. | Reframe to prove LLM/advisory outputs cannot bypass rules/risk/gateway decisions. |
| [tests/integration/test_event_mutation_stress.py](tests/integration/test_event_mutation_stress.py) | MODIFY | Integration coverage remains relevant but needs alignment to current gateway + saga + event-log model. | Update assertions to avoid direct state writes and legacy path assumptions. |
| [tests/integration/test_insight_bridge.py](tests/integration/test_insight_bridge.py) | MODIFY | Touches intelligence/planning behavior that must be explicitly non-authoritative for execution. | Reframe to prove LLM/advisory outputs cannot bypass rules/risk/gateway decisions. |
| [tests/integration/test_intent_lock.py](tests/integration/test_intent_lock.py) | MODIFY | Touches intelligence/planning behavior that must be explicitly non-authoritative for execution. | Reframe to prove LLM/advisory outputs cannot bypass rules/risk/gateway decisions. |
| [tests/integration/test_life_state_model.py](tests/integration/test_life_state_model.py) | MODIFY | Integration coverage remains relevant but needs alignment to current gateway + saga + event-log model. | Update assertions to avoid direct state writes and legacy path assumptions. |
| [tests/integration/test_recommendation_humanization.py](tests/integration/test_recommendation_humanization.py) | MODIFY | Touches intelligence/planning behavior that must be explicitly non-authoritative for execution. | Reframe to prove LLM/advisory outputs cannot bypass rules/risk/gateway decisions. |
| [tests/integration/test_root_ui_routing.py](tests/integration/test_root_ui_routing.py) | MODIFY | Integration/UI simulation coverage is useful but not currently framed around RFC-001 core invariants. | Trim to contract-level assertions and remove demo-style/non-deterministic expectations. |
| [tests/integration/test_sample_household_os_outputs.py](tests/integration/test_sample_household_os_outputs.py) | MODIFY | Integration/UI simulation coverage is useful but not currently framed around RFC-001 core invariants. | Trim to contract-level assertions and remove demo-style/non-deterministic expectations. |
| [tests/integration/test_ui_simulation_endpoints.py](tests/integration/test_ui_simulation_endpoints.py) | MODIFY | Integration/UI simulation coverage is useful but not currently framed around RFC-001 core invariants. | Trim to contract-level assertions and remove demo-style/non-deterministic expectations. |
| [tests/legacy/p1_verification/test_auth_lifecycle.py](tests/legacy/p1_verification/test_auth_lifecycle.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/p1_verification/test_chaos_concurrency.py](tests/legacy/p1_verification/test_chaos_concurrency.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/p1_verification/test_e2e_integration.py](tests/legacy/p1_verification/test_e2e_integration.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/p1_verification/test_event_bus_correctness.py](tests/legacy/p1_verification/test_event_bus_correctness.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/p1_verification/test_idempotency_correctness.py](tests/legacy/p1_verification/test_idempotency_correctness.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/p1_verification/test_llm_gateway_failures.py](tests/legacy/p1_verification/test_llm_gateway_failures.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_actor_type_enforcement.py](tests/legacy/test_actor_type_enforcement.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_adapter_governance.py](tests/legacy/test_adapter_governance.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_asgi_admission.py](tests/legacy/test_asgi_admission.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_auth_realtime_guards.py](tests/legacy/test_auth_realtime_guards.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_auth_router.py](tests/legacy/test_auth_router.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_backpressure_middleware.py](tests/legacy/test_backpressure_middleware.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_behavior_feedback.py](tests/legacy/test_behavior_feedback.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_calendar_events_runtime.py](tests/legacy/test_calendar_events_runtime.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_chat_events_runtime.py](tests/legacy/test_chat_events_runtime.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_col.py](tests/legacy/test_col.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_cqrs_lifecycle_invariants.py](tests/legacy/test_cqrs_lifecycle_invariants.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_email_action_summary.py](tests/legacy/test_email_action_summary.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_email_priority_llm.py](tests/legacy/test_email_priority_llm.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_email_service_actions.py](tests/legacy/test_email_service_actions.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_env_bootstrap.py](tests/legacy/test_env_bootstrap.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_evaluation_endpoints.py](tests/legacy/test_evaluation_endpoints.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_event_adapter.py](tests/legacy/test_event_adapter.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_event_windowing.py](tests/legacy/test_event_windowing.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_external_event_normalization.py](tests/legacy/test_external_event_normalization.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_feature_flags.py](tests/legacy/test_feature_flags.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_frontend_runtime_contract.py](tests/legacy/test_frontend_runtime_contract.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_fsm_non_authority.py](tests/legacy/test_fsm_non_authority.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_google_calendar_full_external_validation.py](tests/legacy/test_google_calendar_full_external_validation.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_google_calendar_integration_surface.py](tests/legacy/test_google_calendar_integration_surface.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_google_calendar_oauth.py](tests/legacy/test_google_calendar_oauth.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_google_oauth_env_config.py](tests/legacy/test_google_oauth_env_config.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_google_oauth_missing_config_safe_boot.py](tests/legacy/test_google_oauth_missing_config_safe_boot.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_household_os.py](tests/legacy/test_household_os.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_household_os_runtime.py](tests/legacy/test_household_os_runtime.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_household_state.py](tests/legacy/test_household_state.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_household_state_manager.py](tests/legacy/test_household_state_manager.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_identity_events_runtime.py](tests/legacy/test_identity_events_runtime.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_identity_layer.py](tests/legacy/test_identity_layer.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_ingestion_events_runtime.py](tests/legacy/test_ingestion_events_runtime.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_ingestion_router.py](tests/legacy/test_ingestion_router.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_integration_architecture_guard.py](tests/legacy/test_integration_architecture_guard.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_integration_core.py](tests/legacy/test_integration_core.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_integration_identity_system.py](tests/legacy/test_integration_identity_system.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_intent_contract.py](tests/legacy/test_intent_contract.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_intent_resolver.py](tests/legacy/test_intent_resolver.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_lifecycle_integrity_invariants.py](tests/legacy/test_lifecycle_integrity_invariants.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_oauth_credential_store.py](tests/legacy/test_oauth_credential_store.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_operational_mode.py](tests/legacy/test_operational_mode.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_outlook_calendar_provider.py](tests/legacy/test_outlook_calendar_provider.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_pantry_router.py](tests/legacy/test_pantry_router.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_policy_guardrails.py](tests/legacy/test_policy_guardrails.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_provider_email_adapter.py](tests/legacy/test_provider_email_adapter.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_provider_registry.py](tests/legacy/test_provider_registry.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_single_pipeline_enforcement.py](tests/legacy/test_single_pipeline_enforcement.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_sse_canonical_only.py](tests/legacy/test_sse_canonical_only.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_state_builder_multi_provider.py](tests/legacy/test_state_builder_multi_provider.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_state_machine.py](tests/legacy/test_state_machine.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_task_service_events.py](tests/legacy/test_task_service_events.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_time_normalization.py](tests/legacy/test_time_normalization.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_trust_boundary_enforcement.py](tests/legacy/test_trust_boundary_enforcement.py) | MODIFY | Covers important domain behavior but still tied to legacy module paths/assumptions. | Port to app.main/household_os execution-gateway paths and canonical event-log assertions. |
| [tests/legacy/test_ui_bootstrap_router.py](tests/legacy/test_ui_bootstrap_router.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_unified_trust_boundary.py](tests/legacy/test_unified_trust_boundary.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/legacy/test_xai_layer.py](tests/legacy/test_xai_layer.py) | MODIFY | Likely still useful but anchored to legacy imports/routes or pre-saga execution assumptions. | Refactor to RFC-001 contracts: gateway mandatory, event-log truth, advisory LLM only. |
| [tests/system/test_intent_confidence_router.py](tests/system/test_intent_confidence_router.py) | MODIFY | System intent routing tests are valuable but can imply LLM-centric routing authority. | Constrain tests to advisory classification only; assert execution authority remains rules/risk gated. |
| [tests/legacy/decision/test_decision_engine.py](tests/legacy/decision/test_decision_engine.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_assistant_core.py](tests/legacy/test_assistant_core.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_assistant_runtime.py](tests/legacy/test_assistant_runtime.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_assistant_runtime_router.py](tests/legacy/test_assistant_runtime_router.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_brief_builder.py](tests/legacy/test_brief_builder.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_brief_renderer_v1.py](tests/legacy/test_brief_renderer_v1.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_daily_loop_engine.py](tests/legacy/test_daily_loop_engine.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_decision_engine.py](tests/legacy/test_decision_engine.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_decision_engine_integration.py](tests/legacy/test_decision_engine_integration.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_manual_priority_adapter.py](tests/legacy/test_manual_priority_adapter.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_output_governor.py](tests/legacy/test_output_governor.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/legacy/test_recommendation_builder.py](tests/legacy/test_recommendation_builder.py) | DELETE | Targets superseded pre-RFC component boundaries (direct assistant/decision/brief layer assumptions). | Delete with cluster pruning; preserve only gateway + event-log invariant coverage. |
| [tests/chaos/test_household_simulation.py](tests/chaos/test_household_simulation.py) | RISK | Chaos/torture simulation with timing-sensitive behavior; useful for resilience, but flaky/long for normal CI. | Move to scheduled nightly chaos pipeline with explicit timeout budget and stability baseline. |
| [tests/chaos/test_llm_gateway_runtime_hardening.py](tests/chaos/test_llm_gateway_runtime_hardening.py) | RISK | Concurrency and cache timing assertions are valuable but can flake under variable runtime scheduling. | Keep as non-blocking reliability suite; enforce retries or isolate with deterministic fakes. |
| [tests/chaos/test_p0_torture.py](tests/chaos/test_p0_torture.py) | RISK | Extreme reconnect/replay/write-storm torture test; high runtime and instability risk. | Keep out of default CI; run in dedicated soak/torture stage. |
| [tests/chaos/test_xai_harness_integration.py](tests/chaos/test_xai_harness_integration.py) | RISK | Broad matrix chaos integration with many scenarios; likely slow and noisy for fast feedback loops. | Run as periodic resilience certification, not per-commit gate. |
| [tests/integration/test_full_suite_integrity.py](tests/integration/test_full_suite_integrity.py) | RISK | Recursive full-suite invocation from inside pytest can dramatically inflate runtime and duplicate coverage. | Disable in standard CI; run only as explicit pipeline job when requested. |
| [tests/integration/test_live_orchestration_simulation.py](tests/integration/test_live_orchestration_simulation.py) | RISK | End-to-end simulation harness is useful but likely slow and timing-sensitive. | Shift to nightly/system-stage runs with strict wall-clock limits. |
| [tests/legacy/test_security_verification_harness.py](tests/legacy/test_security_verification_harness.py) | RISK | Large harness-style security verification can overlap with torture/integration checks and run long. | Extract critical assertions into focused tests; keep bulk harness for scheduled audits. |
| [tests/legacy/test_system_stability_lock.py](tests/legacy/test_system_stability_lock.py) | RISK | Stability-lock style checks are often timing dependent and environment sensitive. | Retain as review-only guard; avoid blocking fast CI. |

### C. Dead test clusters

- Legacy assistant runtime + routing cluster (4 files)
  - [tests/legacy/test_assistant_core.py](tests/legacy/test_assistant_core.py)
  - [tests/legacy/test_assistant_runtime.py](tests/legacy/test_assistant_runtime.py)
  - [tests/legacy/test_assistant_runtime_router.py](tests/legacy/test_assistant_runtime_router.py)
  - [tests/legacy/test_daily_loop_engine.py](tests/legacy/test_daily_loop_engine.py)
- Legacy decision engine cluster (3 files)
  - [tests/legacy/decision/test_decision_engine.py](tests/legacy/decision/test_decision_engine.py)
  - [tests/legacy/test_decision_engine.py](tests/legacy/test_decision_engine.py)
  - [tests/legacy/test_decision_engine_integration.py](tests/legacy/test_decision_engine_integration.py)
- Legacy brief/recommendation composition cluster (5 files)
  - [tests/legacy/test_brief_builder.py](tests/legacy/test_brief_builder.py)
  - [tests/legacy/test_brief_renderer_v1.py](tests/legacy/test_brief_renderer_v1.py)
  - [tests/legacy/test_recommendation_builder.py](tests/legacy/test_recommendation_builder.py)
  - [tests/legacy/test_output_governor.py](tests/legacy/test_output_governor.py)
  - [tests/legacy/test_manual_priority_adapter.py](tests/legacy/test_manual_priority_adapter.py)

### D. Minimal surviving test suite definition

1. Commit-blocking core invariant suite
   - [tests/integration/architecture/test_dependency_graph.py](tests/integration/architecture/test_dependency_graph.py)
   - [tests/integration/architecture/test_fetch_boundary.py](tests/integration/architecture/test_fetch_boundary.py)
   - [tests/integration/architecture/test_single_fetch_source.py](tests/integration/architecture/test_single_fetch_source.py)
   - [tests/integration/test_actor_context_propagation.py](tests/integration/test_actor_context_propagation.py)
   - [tests/integration/test_boundary_enforcement.py](tests/integration/test_boundary_enforcement.py)
   - [tests/integration/test_event_replay_integrity.py](tests/integration/test_event_replay_integrity.py)
   - [tests/integration/test_intelligence_analytics_dls.py](tests/integration/test_intelligence_analytics_dls.py)
   - [tests/integration/test_intelligence_routing_resilience.py](tests/integration/test_intelligence_routing_resilience.py)
   - [tests/integration/test_lifecycle_surface_consistency_guard.py](tests/integration/test_lifecycle_surface_consistency_guard.py)
   - [tests/integration/test_no_legacy_lifecycle_strings.py](tests/integration/test_no_legacy_lifecycle_strings.py)
   - [tests/integration/test_no_raw_lifecycle_strings.py](tests/integration/test_no_raw_lifecycle_strings.py)
   - [tests/integration/test_persistence_roundtrip_integrity.py](tests/integration/test_persistence_roundtrip_integrity.py)
   - [tests/integration/test_policy_engine.py](tests/integration/test_policy_engine.py)
   - [tests/system/test_architecture_suite.py](tests/system/test_architecture_suite.py)
   - [tests/system/test_event_sourcing.py](tests/system/test_event_sourcing.py)
   - [tests/system/test_event_store_invariants.py](tests/system/test_event_store_invariants.py)
   - [tests/test_action_event_contracts.py](tests/test_action_event_contracts.py)
   - [tests/test_fsm_immutability_enforcement.py](tests/test_fsm_immutability_enforcement.py)
   - [tests/test_governance_gates.py](tests/test_governance_gates.py)
   - [tests/test_hard_freeze_regression.py](tests/test_hard_freeze_regression.py)
   - [tests/test_invariance_enforcement.py](tests/test_invariance_enforcement.py)
   - [tests/test_layer_redundancy_guard.py](tests/test_layer_redundancy_guard.py)
   - [tests/test_migration_cleanliness.py](tests/test_migration_cleanliness.py)
   - [tests/test_no_direct_broadcaster_bypass.py](tests/test_no_direct_broadcaster_bypass.py)
   - [tests/test_sse_event_closure.py](tests/test_sse_event_closure.py)
   - [tests/test_static_silent_mutations.py](tests/test_static_silent_mutations.py)
   - [tests/test_trust_surface_closure.py](tests/test_trust_surface_closure.py)
   - [tests/test_trust_surface_final_closure.py](tests/test_trust_surface_final_closure.py)
   - [tests/test_ui_canonical_wiring_guard.py](tests/test_ui_canonical_wiring_guard.py)
   - [tests/unit/test_lifecycle_contract_boundary.py](tests/unit/test_lifecycle_contract_boundary.py)
2. Port-required suite (must pass before deleting legacy layer)
   - [tests/integration/test_brief_evaluation.py](tests/integration/test_brief_evaluation.py)
   - [tests/integration/test_event_mutation_stress.py](tests/integration/test_event_mutation_stress.py)
   - [tests/integration/test_insight_bridge.py](tests/integration/test_insight_bridge.py)
   - [tests/integration/test_intent_lock.py](tests/integration/test_intent_lock.py)
   - [tests/integration/test_life_state_model.py](tests/integration/test_life_state_model.py)
   - [tests/integration/test_recommendation_humanization.py](tests/integration/test_recommendation_humanization.py)
   - [tests/integration/test_root_ui_routing.py](tests/integration/test_root_ui_routing.py)
   - [tests/integration/test_sample_household_os_outputs.py](tests/integration/test_sample_household_os_outputs.py)
   - [tests/integration/test_ui_simulation_endpoints.py](tests/integration/test_ui_simulation_endpoints.py)
   - [tests/legacy/p1_verification/test_auth_lifecycle.py](tests/legacy/p1_verification/test_auth_lifecycle.py)
   - [tests/legacy/p1_verification/test_chaos_concurrency.py](tests/legacy/p1_verification/test_chaos_concurrency.py)
   - [tests/legacy/p1_verification/test_e2e_integration.py](tests/legacy/p1_verification/test_e2e_integration.py)
   - [tests/legacy/p1_verification/test_event_bus_correctness.py](tests/legacy/p1_verification/test_event_bus_correctness.py)
   - [tests/legacy/p1_verification/test_idempotency_correctness.py](tests/legacy/p1_verification/test_idempotency_correctness.py)
   - [tests/legacy/p1_verification/test_llm_gateway_failures.py](tests/legacy/p1_verification/test_llm_gateway_failures.py)
   - [tests/legacy/test_actor_type_enforcement.py](tests/legacy/test_actor_type_enforcement.py)
   - [tests/legacy/test_adapter_governance.py](tests/legacy/test_adapter_governance.py)
   - [tests/legacy/test_asgi_admission.py](tests/legacy/test_asgi_admission.py)
   - [tests/legacy/test_auth_realtime_guards.py](tests/legacy/test_auth_realtime_guards.py)
   - [tests/legacy/test_auth_router.py](tests/legacy/test_auth_router.py)
   - [tests/legacy/test_backpressure_middleware.py](tests/legacy/test_backpressure_middleware.py)
   - [tests/legacy/test_behavior_feedback.py](tests/legacy/test_behavior_feedback.py)
   - [tests/legacy/test_calendar_events_runtime.py](tests/legacy/test_calendar_events_runtime.py)
   - [tests/legacy/test_chat_events_runtime.py](tests/legacy/test_chat_events_runtime.py)
   - [tests/legacy/test_col.py](tests/legacy/test_col.py)
   - [tests/legacy/test_cqrs_lifecycle_invariants.py](tests/legacy/test_cqrs_lifecycle_invariants.py)
   - [tests/legacy/test_email_action_summary.py](tests/legacy/test_email_action_summary.py)
   - [tests/legacy/test_email_priority_llm.py](tests/legacy/test_email_priority_llm.py)
   - [tests/legacy/test_email_service_actions.py](tests/legacy/test_email_service_actions.py)
   - [tests/legacy/test_env_bootstrap.py](tests/legacy/test_env_bootstrap.py)
   - [tests/legacy/test_evaluation_endpoints.py](tests/legacy/test_evaluation_endpoints.py)
   - [tests/legacy/test_event_adapter.py](tests/legacy/test_event_adapter.py)
   - [tests/legacy/test_event_windowing.py](tests/legacy/test_event_windowing.py)
   - [tests/legacy/test_external_event_normalization.py](tests/legacy/test_external_event_normalization.py)
   - [tests/legacy/test_feature_flags.py](tests/legacy/test_feature_flags.py)
   - [tests/legacy/test_frontend_runtime_contract.py](tests/legacy/test_frontend_runtime_contract.py)
   - [tests/legacy/test_fsm_non_authority.py](tests/legacy/test_fsm_non_authority.py)
   - [tests/legacy/test_google_calendar_full_external_validation.py](tests/legacy/test_google_calendar_full_external_validation.py)
   - [tests/legacy/test_google_calendar_integration_surface.py](tests/legacy/test_google_calendar_integration_surface.py)
   - [tests/legacy/test_google_calendar_oauth.py](tests/legacy/test_google_calendar_oauth.py)
   - [tests/legacy/test_google_oauth_env_config.py](tests/legacy/test_google_oauth_env_config.py)
   - [tests/legacy/test_google_oauth_missing_config_safe_boot.py](tests/legacy/test_google_oauth_missing_config_safe_boot.py)
   - [tests/legacy/test_household_os.py](tests/legacy/test_household_os.py)
   - [tests/legacy/test_household_os_runtime.py](tests/legacy/test_household_os_runtime.py)
   - [tests/legacy/test_household_state.py](tests/legacy/test_household_state.py)
   - [tests/legacy/test_household_state_manager.py](tests/legacy/test_household_state_manager.py)
   - [tests/legacy/test_identity_events_runtime.py](tests/legacy/test_identity_events_runtime.py)
   - [tests/legacy/test_identity_layer.py](tests/legacy/test_identity_layer.py)
   - [tests/legacy/test_ingestion_events_runtime.py](tests/legacy/test_ingestion_events_runtime.py)
   - [tests/legacy/test_ingestion_router.py](tests/legacy/test_ingestion_router.py)
   - [tests/legacy/test_integration_architecture_guard.py](tests/legacy/test_integration_architecture_guard.py)
   - [tests/legacy/test_integration_core.py](tests/legacy/test_integration_core.py)
   - [tests/legacy/test_integration_identity_system.py](tests/legacy/test_integration_identity_system.py)
   - [tests/legacy/test_intent_contract.py](tests/legacy/test_intent_contract.py)
   - [tests/legacy/test_intent_resolver.py](tests/legacy/test_intent_resolver.py)
   - [tests/legacy/test_lifecycle_integrity_invariants.py](tests/legacy/test_lifecycle_integrity_invariants.py)
   - [tests/legacy/test_oauth_credential_store.py](tests/legacy/test_oauth_credential_store.py)
   - [tests/legacy/test_operational_mode.py](tests/legacy/test_operational_mode.py)
   - [tests/legacy/test_outlook_calendar_provider.py](tests/legacy/test_outlook_calendar_provider.py)
   - [tests/legacy/test_pantry_router.py](tests/legacy/test_pantry_router.py)
   - [tests/legacy/test_policy_guardrails.py](tests/legacy/test_policy_guardrails.py)
   - [tests/legacy/test_provider_email_adapter.py](tests/legacy/test_provider_email_adapter.py)
   - [tests/legacy/test_provider_registry.py](tests/legacy/test_provider_registry.py)
   - [tests/legacy/test_single_pipeline_enforcement.py](tests/legacy/test_single_pipeline_enforcement.py)
   - [tests/legacy/test_sse_canonical_only.py](tests/legacy/test_sse_canonical_only.py)
   - [tests/legacy/test_state_builder_multi_provider.py](tests/legacy/test_state_builder_multi_provider.py)
   - [tests/legacy/test_state_machine.py](tests/legacy/test_state_machine.py)
   - [tests/legacy/test_task_service_events.py](tests/legacy/test_task_service_events.py)
   - [tests/legacy/test_time_normalization.py](tests/legacy/test_time_normalization.py)
   - [tests/legacy/test_trust_boundary_enforcement.py](tests/legacy/test_trust_boundary_enforcement.py)
   - [tests/legacy/test_ui_bootstrap_router.py](tests/legacy/test_ui_bootstrap_router.py)
   - [tests/legacy/test_unified_trust_boundary.py](tests/legacy/test_unified_trust_boundary.py)
   - [tests/legacy/test_xai_layer.py](tests/legacy/test_xai_layer.py)
   - [tests/system/test_intent_confidence_router.py](tests/system/test_intent_confidence_router.py)
3. Quarantined reliability suite (scheduled/nightly, non-blocking)
   - [tests/chaos/test_household_simulation.py](tests/chaos/test_household_simulation.py)
   - [tests/chaos/test_llm_gateway_runtime_hardening.py](tests/chaos/test_llm_gateway_runtime_hardening.py)
   - [tests/chaos/test_p0_torture.py](tests/chaos/test_p0_torture.py)
   - [tests/chaos/test_xai_harness_integration.py](tests/chaos/test_xai_harness_integration.py)
   - [tests/integration/test_full_suite_integrity.py](tests/integration/test_full_suite_integrity.py)
   - [tests/integration/test_live_orchestration_simulation.py](tests/integration/test_live_orchestration_simulation.py)
   - [tests/legacy/test_security_verification_harness.py](tests/legacy/test_security_verification_harness.py)
   - [tests/legacy/test_system_stability_lock.py](tests/legacy/test_system_stability_lock.py)
4. Deletion candidate suite (remove after confirming no remaining imports/use)
   - [tests/legacy/decision/test_decision_engine.py](tests/legacy/decision/test_decision_engine.py)
   - [tests/legacy/test_assistant_core.py](tests/legacy/test_assistant_core.py)
   - [tests/legacy/test_assistant_runtime.py](tests/legacy/test_assistant_runtime.py)
   - [tests/legacy/test_assistant_runtime_router.py](tests/legacy/test_assistant_runtime_router.py)
   - [tests/legacy/test_brief_builder.py](tests/legacy/test_brief_builder.py)
   - [tests/legacy/test_brief_renderer_v1.py](tests/legacy/test_brief_renderer_v1.py)
   - [tests/legacy/test_daily_loop_engine.py](tests/legacy/test_daily_loop_engine.py)
   - [tests/legacy/test_decision_engine.py](tests/legacy/test_decision_engine.py)
   - [tests/legacy/test_decision_engine_integration.py](tests/legacy/test_decision_engine_integration.py)
   - [tests/legacy/test_manual_priority_adapter.py](tests/legacy/test_manual_priority_adapter.py)
   - [tests/legacy/test_output_governor.py](tests/legacy/test_output_governor.py)
   - [tests/legacy/test_recommendation_builder.py](tests/legacy/test_recommendation_builder.py)