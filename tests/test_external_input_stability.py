from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from core.contracts.system_contract_validator import validate_system_contract


pytestmark = [pytest.mark.ci_gate, pytest.mark.migration, pytest.mark.reliability]


@dataclass(frozen=True)
class IngestionDecision:
    event_id: str
    decision: str
    reason: str


class ExternalInputIngestionHarness:
    _VALID_TYPES = {"task.create", "task.update", "task.complete"}
    _REQUIRED_KEYS = {
        "event_id",
        "idempotency_key",
        "household_id",
        "event_type",
        "sequence",
        "timestamp",
        "payload",
    }

    def simulate(self, inbound_events: list[dict[str, Any]]) -> dict[str, Any]:
        accepted_events: list[dict[str, Any]] = []
        rejected_events: list[dict[str, Any]] = []
        decisions: list[IngestionDecision] = []
        seen_idempotency_keys: set[str] = set()
        highest_sequence_seen = -1

        for index, raw in enumerate(inbound_events):
            valid, reason = self._validate_event(raw)
            if not valid:
                event_id = str(raw.get("event_id") or f"invalid-{index}") if isinstance(raw, dict) else f"invalid-{index}"
                rejected_events.append(
                    {
                        "event_id": event_id,
                        "reason": reason,
                        "raw": raw,
                    }
                )
                decisions.append(IngestionDecision(event_id=event_id, decision="block", reason=reason))
                continue

            event_id = str(raw["event_id"])
            idempotency_key = str(raw["idempotency_key"])
            sequence = int(raw["sequence"])

            if idempotency_key in seen_idempotency_keys:
                decisions.append(IngestionDecision(event_id=event_id, decision="block", reason="duplicate_input"))
                continue

            delayed = sequence < highest_sequence_seen
            highest_sequence_seen = max(highest_sequence_seen, sequence)
            seen_idempotency_keys.add(idempotency_key)

            accepted = {
                **raw,
                "_ingest_index": index,
                "_delayed": delayed,
            }
            accepted_events.append(accepted)
            decisions.append(
                IngestionDecision(
                    event_id=event_id,
                    decision="allow",
                    reason="delayed_input" if delayed else "ingest_ok",
                )
            )

        runtime_projection = self._project_state(accepted_events)
        replay_projection = self._project_state(list(reversed(accepted_events)))
        decision_payload = [
            {
                "event_id": item.event_id,
                "decision": item.decision,
                "reason": item.reason,
            }
            for item in decisions
        ]

        runtime_output = self._build_output(runtime_projection, decision_payload)
        replay_output = self._build_output(replay_projection, decision_payload)
        api_response = {"projection": dict(runtime_output["projection"])}

        contract = validate_system_contract(
            event_stream=self._ordered_events(accepted_events),
            runtime_output=runtime_output,
            replay_output=replay_output,
            control_plane_decisions=runtime_output["control_plane_decisions"],
            api_response=api_response,
        )

        return {
            "accepted_events": accepted_events,
            "rejected_events": rejected_events,
            "runtime_output": runtime_output,
            "replay_output": replay_output,
            "api_response": api_response,
            "ordered_event_ids": [item["event_id"] for item in self._ordered_events(accepted_events)],
            "contract": contract,
        }

    def _validate_event(self, raw: Any) -> tuple[bool, str]:
        if not isinstance(raw, dict):
            return False, "malformed_input:not_object"

        missing = sorted(self._REQUIRED_KEYS - set(raw.keys()))
        if missing:
            return False, f"malformed_input:missing_fields:{','.join(missing)}"

        if str(raw.get("event_type") or "") not in self._VALID_TYPES:
            return False, "malformed_input:invalid_event_type"

        try:
            sequence = int(raw.get("sequence"))
        except (TypeError, ValueError):
            return False, "malformed_input:invalid_sequence"

        if sequence < 0:
            return False, "malformed_input:negative_sequence"

        payload = raw.get("payload")
        if not isinstance(payload, dict):
            return False, "malformed_input:payload_not_object"

        task_id = str(payload.get("task_id") or "").strip()
        if not task_id:
            return False, "malformed_input:missing_task_id"

        event_type = str(raw.get("event_type") or "")
        if event_type in {"task.create", "task.update"}:
            title = str(payload.get("title") or "").strip()
            if not title:
                return False, "malformed_input:missing_title"

        return True, "valid"

    @staticmethod
    def _ordered_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(
            events,
            key=lambda item: (
                int(item["sequence"]),
                str(item["timestamp"]),
                str(item["event_id"]),
                int(item.get("_ingest_index") or 0),
            ),
        )

    def _project_state(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        tasks: dict[str, dict[str, Any]] = {}

        for event in self._ordered_events(events):
            payload = event["payload"]
            task_id = str(payload["task_id"])
            event_type = str(event["event_type"])

            if event_type == "task.create":
                tasks[task_id] = {
                    "task_id": task_id,
                    "title": str(payload["title"]),
                    "status": "created",
                }
            elif event_type == "task.update":
                current = tasks.setdefault(
                    task_id,
                    {
                        "task_id": task_id,
                        "title": str(payload["title"]),
                        "status": "created",
                    },
                )
                current["title"] = str(payload["title"])
                current["status"] = "updated"
            elif event_type == "task.complete":
                current = tasks.setdefault(
                    task_id,
                    {
                        "task_id": task_id,
                        "title": str(payload.get("title") or ""),
                        "status": "created",
                    },
                )
                current["status"] = "completed"

        return {
            "tasks": tasks,
        }

    @staticmethod
    def _build_output(projection: dict[str, Any], control_plane_decisions: list[dict[str, Any]]) -> dict[str, Any]:
        fsm_state = {
            "tasks": {
                task_id: {
                    "current_state": str(task["status"]),
                }
                for task_id, task in projection["tasks"].items()
            }
        }

        return {
            "fsm_state": fsm_state,
            "projection": projection,
            "control_plane_decisions": control_plane_decisions,
        }


def _event(
    *,
    event_id: str,
    idempotency_key: str,
    event_type: str,
    sequence: int,
    timestamp: str,
    task_id: str,
    title: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"task_id": task_id}
    if title is not None:
        payload["title"] = title

    return {
        "event_id": event_id,
        "idempotency_key": idempotency_key,
        "household_id": "household-ext-ingest",
        "event_type": event_type,
        "sequence": sequence,
        "timestamp": timestamp,
        "payload": payload,
    }


def test_duplicate_inputs_are_idempotent() -> None:
    harness = ExternalInputIngestionHarness()
    result = harness.simulate(
        [
            _event(
                event_id="evt-1",
                idempotency_key="idem-task-1",
                event_type="task.create",
                sequence=1,
                timestamp="2026-04-30T12:00:00Z",
                task_id="task-1",
                title="Create task",
            ),
            _event(
                event_id="evt-2-dup",
                idempotency_key="idem-task-1",
                event_type="task.create",
                sequence=1,
                timestamp="2026-04-30T12:00:00Z",
                task_id="task-1",
                title="Create task duplicate",
            ),
            _event(
                event_id="evt-3",
                idempotency_key="idem-task-1-update",
                event_type="task.update",
                sequence=2,
                timestamp="2026-04-30T12:00:01Z",
                task_id="task-1",
                title="Updated title",
            ),
        ]
    )

    block_reasons = [
        item["reason"]
        for item in result["runtime_output"]["control_plane_decisions"]
        if item["decision"] == "block"
    ]

    assert len(result["accepted_events"]) == 2
    assert "duplicate_input" in block_reasons
    assert result["runtime_output"]["projection"]["tasks"]["task-1"]["title"] == "Updated title"
    assert result["contract"] == {"matches": True, "differences": []}


def test_delayed_inputs_do_not_corrupt_state() -> None:
    harness = ExternalInputIngestionHarness()
    result = harness.simulate(
        [
            _event(
                event_id="evt-2",
                idempotency_key="idem-2",
                event_type="task.update",
                sequence=2,
                timestamp="2026-04-30T12:00:02Z",
                task_id="task-delay",
                title="Updated late",
            ),
            _event(
                event_id="evt-1-delayed",
                idempotency_key="idem-1",
                event_type="task.create",
                sequence=1,
                timestamp="2026-04-30T12:00:01Z",
                task_id="task-delay",
                title="Created first",
            ),
            _event(
                event_id="evt-3",
                idempotency_key="idem-3",
                event_type="task.complete",
                sequence=3,
                timestamp="2026-04-30T12:00:03Z",
                task_id="task-delay",
            ),
        ]
    )

    allow_reasons = [
        item["reason"]
        for item in result["runtime_output"]["control_plane_decisions"]
        if item["decision"] == "allow"
    ]

    task = result["runtime_output"]["projection"]["tasks"]["task-delay"]
    assert "delayed_input" in allow_reasons
    assert task["status"] == "completed"
    assert task["title"] == "Updated late"
    assert result["contract"]["matches"] is True


def test_out_of_order_inputs_resolve_correctly() -> None:
    harness = ExternalInputIngestionHarness()

    canonical = [
        _event(
            event_id="evt-1",
            idempotency_key="idem-1",
            event_type="task.create",
            sequence=1,
            timestamp="2026-04-30T12:00:01Z",
            task_id="task-order",
            title="Initial",
        ),
        _event(
            event_id="evt-2",
            idempotency_key="idem-2",
            event_type="task.update",
            sequence=2,
            timestamp="2026-04-30T12:00:02Z",
            task_id="task-order",
            title="Refined",
        ),
        _event(
            event_id="evt-3",
            idempotency_key="idem-3",
            event_type="task.complete",
            sequence=3,
            timestamp="2026-04-30T12:00:03Z",
            task_id="task-order",
        ),
    ]

    reordered = [canonical[2], canonical[0], canonical[1]]

    first = harness.simulate(canonical)
    second = harness.simulate(reordered)

    assert first["ordered_event_ids"] == second["ordered_event_ids"]
    assert first["runtime_output"]["projection"] == second["runtime_output"]["projection"]
    assert first["runtime_output"]["fsm_state"] == second["runtime_output"]["fsm_state"]
    assert first["replay_output"]["projection"] == second["replay_output"]["projection"]
    assert first["replay_output"]["fsm_state"] == second["replay_output"]["fsm_state"]
    assert first["contract"] == second["contract"] == {"matches": True, "differences": []}


def test_malformed_inputs_are_rejected() -> None:
    harness = ExternalInputIngestionHarness()

    malformed_missing_fields = {
        "event_id": "bad-1",
        "event_type": "task.create",
        "sequence": 1,
        "timestamp": "2026-04-30T12:00:00Z",
        "payload": {"task_id": "bad-task", "title": "bad"},
    }
    malformed_wrong_payload = {
        "event_id": "bad-2",
        "idempotency_key": "idem-bad-2",
        "household_id": "household-ext-ingest",
        "event_type": "task.create",
        "sequence": "NaN",
        "timestamp": "2026-04-30T12:00:00Z",
        "payload": "not-a-dict",
    }

    result = harness.simulate(
        [
            malformed_missing_fields,
            malformed_wrong_payload,
            _event(
                event_id="evt-valid",
                idempotency_key="idem-valid",
                event_type="task.create",
                sequence=1,
                timestamp="2026-04-30T12:00:01Z",
                task_id="task-valid",
                title="Valid",
            ),
        ]
    )

    rejected_reasons = [item["reason"] for item in result["rejected_events"]]

    assert len(result["rejected_events"]) == 2
    assert any(reason.startswith("malformed_input:missing_fields") for reason in rejected_reasons)
    assert "malformed_input:invalid_sequence" in rejected_reasons
    assert len(result["accepted_events"]) == 1
    assert result["runtime_output"]["projection"]["tasks"]["task-valid"]["title"] == "Valid"
    assert result["contract"] == {"matches": True, "differences": []}


def test_mixed_input_conditions_remain_stable() -> None:
    harness = ExternalInputIngestionHarness()

    first_run_inputs = [
        _event(
            event_id="mix-2",
            idempotency_key="mix-idem-2",
            event_type="task.update",
            sequence=2,
            timestamp="2026-04-30T12:01:02Z",
            task_id="task-mixed",
            title="Updated mixed",
        ),
        {
            "event_id": "mix-bad",
            "idempotency_key": "mix-bad",
            "household_id": "household-ext-ingest",
            "event_type": "task.create",
            "sequence": 1,
            "timestamp": "2026-04-30T12:01:00Z",
            "payload": {"task_id": "task-mixed"},
        },
        _event(
            event_id="mix-1",
            idempotency_key="mix-idem-1",
            event_type="task.create",
            sequence=1,
            timestamp="2026-04-30T12:01:01Z",
            task_id="task-mixed",
            title="Created mixed",
        ),
        _event(
            event_id="mix-1-dup",
            idempotency_key="mix-idem-1",
            event_type="task.create",
            sequence=1,
            timestamp="2026-04-30T12:01:01Z",
            task_id="task-mixed",
            title="Created mixed duplicate",
        ),
        _event(
            event_id="mix-3",
            idempotency_key="mix-idem-3",
            event_type="task.complete",
            sequence=3,
            timestamp="2026-04-30T12:01:03Z",
            task_id="task-mixed",
        ),
    ]

    second_run_inputs = list(reversed(first_run_inputs))

    first = harness.simulate(first_run_inputs)
    second = harness.simulate(second_run_inputs)

    assert first["runtime_output"]["projection"] == second["runtime_output"]["projection"]
    assert first["runtime_output"]["fsm_state"] == second["runtime_output"]["fsm_state"]
    assert first["replay_output"]["projection"] == second["replay_output"]["projection"]
    assert first["replay_output"]["fsm_state"] == second["replay_output"]["fsm_state"]

    first_duplicate_blocks = [
        item
        for item in first["runtime_output"]["control_plane_decisions"]
        if item["decision"] == "block" and item["reason"] == "duplicate_input"
    ]
    second_duplicate_blocks = [
        item
        for item in second["runtime_output"]["control_plane_decisions"]
        if item["decision"] == "block" and item["reason"] == "duplicate_input"
    ]

    assert len(first_duplicate_blocks) == 1
    assert len(second_duplicate_blocks) == 1
    assert first["runtime_output"]["projection"]["tasks"]["task-mixed"]["status"] == "completed"
    assert first["contract"] == second["contract"] == {"matches": True, "differences": []}
