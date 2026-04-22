# EXECUTION_MAP

Total traces analyzed: 15

## Function Heat

| Function | Count | Unique Callers | Temperature |
| --- | ---: | ---: | --- |
| apps.api.assistant_runtime_router.approve_assistant_action | 0 | 0 | DEAD |
| apps.api.assistant_runtime_router.assistant_today | 0 | 0 | DEAD |
| apps.api.assistant_runtime_router.reject_assistant_action | 0 | 0 | DEAD |
| apps.api.assistant_runtime_router.run_assistant | 0 | 0 | DEAD |
| apps.api.endpoints.operational_router._run_pipeline | 0 | 0 | DEAD |
| apps.api.endpoints.operational_router.get_operational_brief | 0 | 0 | DEAD |
| apps.api.endpoints.operational_router.get_operational_context | 0 | 0 | DEAD |
| apps.api.endpoints.operational_router.run_operational_mode | 0 | 0 | DEAD |
| apps.api.ingestion.service.ingest_email | 1 | 0 | COLD |
| apps.api.ingestion.service.ingest_webhook | 1 | 0 | COLD |
| apps.api.main.create_app.<locals>.ingest_event | 1 | 0 | COLD |
| apps.api.services.event_replay_service.replay_events | 0 | 0 | DEAD |
| apps.api.services.event_replay_service.replay_events_for_household | 0 | 0 | DEAD |
| household_os.runtime.action_pipeline.ActionPipeline.approve_actions | 2 | 1 | COLD |
| household_os.runtime.action_pipeline.ActionPipeline.execute_approved_actions | 2 | 1 | COLD |
| household_os.runtime.action_pipeline.ActionPipeline.register_proposed_action | 7 | 1 | WARM |
| household_os.runtime.action_pipeline.ActionPipeline.reject_action_timeout | 0 | 0 | DEAD |
| household_os.runtime.action_pipeline.ActionPipeline.reject_actions | 0 | 0 | DEAD |
| household_os.runtime.orchestrator.HouseholdOSOrchestrator.approve_and_execute | 1 | 0 | COLD |
| household_os.runtime.orchestrator.HouseholdOSOrchestrator.tick | 5 | 0 | WARM |
| household_os.runtime.state_reducer.replay_events | 2 | 0 | COLD |

## Module Heatmap

| Module | Total Calls | Functions | HOT | WARM | COLD | DEAD | Temperature |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| apps.api.assistant_runtime_router | 0 | 4 | 0 | 0 | 0 | 4 | DEAD |
| apps.api.endpoints.operational_router | 0 | 4 | 0 | 0 | 0 | 4 | DEAD |
| apps.api.ingestion.service | 2 | 2 | 0 | 0 | 2 | 0 | COLD |
| apps.api.main.create_app.<locals> | 1 | 1 | 0 | 0 | 1 | 0 | COLD |
| apps.api.services.event_replay_service | 0 | 2 | 0 | 0 | 0 | 2 | DEAD |
| household_os.runtime.action_pipeline.ActionPipeline | 11 | 5 | 0 | 1 | 2 | 2 | WARM |
| household_os.runtime.orchestrator.HouseholdOSOrchestrator | 6 | 2 | 0 | 1 | 1 | 0 | WARM |
| household_os.runtime.state_reducer | 2 | 1 | 0 | 0 | 1 | 0 | COLD |

## Reachability

### action_pipeline.approve_actions
- household_os.runtime.action_pipeline.ActionPipeline.approve_actions

### action_pipeline.execute_approved_actions
- household_os.runtime.action_pipeline.ActionPipeline.execute_approved_actions

### action_pipeline.register_proposed_action
- household_os.runtime.action_pipeline.ActionPipeline.register_proposed_action

### api.event_ingest
- apps.api.main.create_app.<locals>.ingest_event

### ingestion.email
- apps.api.ingestion.service.ingest_email

### ingestion.webhook
- apps.api.ingestion.service.ingest_webhook

### orchestrator.approve_and_execute
- household_os.runtime.action_pipeline.ActionPipeline.approve_actions
- household_os.runtime.action_pipeline.ActionPipeline.execute_approved_actions
- household_os.runtime.orchestrator.HouseholdOSOrchestrator.approve_and_execute

### orchestrator.tick
- household_os.runtime.action_pipeline.ActionPipeline.register_proposed_action
- household_os.runtime.orchestrator.HouseholdOSOrchestrator.tick

### state_reducer.replay_events
- household_os.runtime.state_reducer.replay_events
