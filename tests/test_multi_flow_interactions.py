from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from core.contracts.system_contract_validator import validate_system_contract


pytestmark = [pytest.mark.ci_gate, pytest.mark.migration, pytest.mark.reliability]


@dataclass(frozen=True)
class FlowMutation:
    flow_id: str
    op_id: str
    tick: int
    kind: str
    resource: str
    payload: dict[str, Any]


@dataclass
class SimulationState:
    tasks: dict[str, dict[str, Any]]
    slots: dict[str, dict[str, Any]]
    sagas: dict[str, dict[str, Any]]
    circuits: set[str]


class MultiFlowSimulationHarness:
    _CONFLICT_KINDS = {"task_upsert", "slot_reserve", "saga_start", "guarded_task"}

    @staticmethod
    def _mutation_order_key(mutation: FlowMutation) -> tuple[str, str]:
        return (mutation.flow_id, mutation.op_id)

    def run(self, mutations: list[FlowMutation]) -> dict[str, Any]:
        by_tick: dict[int, list[FlowMutation]] = {}
        for mutation in mutations:
            by_tick.setdefault(mutation.tick, []).append(mutation)

        state = SimulationState(tasks={}, slots={}, sagas={}, circuits=set())
        events: list[dict[str, Any]] = []
        sequence = 1

        def emit(event_type: str, *, tick: int, flow_id: str, resource: str, payload: dict[str, Any]) -> None:
            nonlocal sequence
            events.append(
                {
                    "event_id": f"evt-{sequence:04d}",
                    "event_type": event_type,
                    "sequence": sequence,
                    "tick": tick,
                    "flow_id": flow_id,
                    "resource": resource,
                    "payload": dict(payload),
                    "source": "tests.multi_flow",
                    "household_id": "household-multi-flow",
                    "timestamp": f"2026-04-30T10:00:{sequence:02d}Z",
                }
            )
            sequence += 1

        for tick in sorted(by_tick.keys()):
            tick_mutations = list(by_tick[tick])

            for circuit_open in sorted(
                [item for item in tick_mutations if item.kind == "circuit_open"],
                key=self._mutation_order_key,
            ):
                if circuit_open.resource not in state.circuits:
                    state.circuits.add(circuit_open.resource)
                    emit(
                        "system.circuit_opened",
                        tick=tick,
                        flow_id=circuit_open.flow_id,
                        resource=circuit_open.resource,
                        payload={"reason": str(circuit_open.payload.get("reason") or "safety_guard")},
                    )
                emit(
                    "control.decision",
                    tick=tick,
                    flow_id=circuit_open.flow_id,
                    resource=circuit_open.resource,
                    payload={"decision": "allow", "reason": "circuit_opened"},
                )

            remaining = [item for item in tick_mutations if item.kind != "circuit_open"]
            grouped: dict[tuple[str, str], list[FlowMutation]] = {}
            for mutation in remaining:
                grouping_key = (
                    mutation.kind,
                    mutation.resource if mutation.kind in self._CONFLICT_KINDS else f"{mutation.flow_id}:{mutation.op_id}",
                )
                grouped.setdefault(grouping_key, []).append(mutation)

            for group_key in sorted(grouped.keys(), key=lambda item: f"{item[0]}::{item[1]}"):
                contenders = sorted(grouped[group_key], key=self._mutation_order_key)
                contender_kind = contenders[0].kind
                resource = contenders[0].resource

                if contender_kind == "saga_commit":
                    for mutation in contenders:
                        saga_id = str(mutation.payload.get("saga_id") or "")
                        saga = state.sagas.get(saga_id)
                        if not saga:
                            emit(
                                "saga.commit_skipped",
                                tick=tick,
                                flow_id=mutation.flow_id,
                                resource=resource,
                                payload={"saga_id": saga_id, "reason": "missing_saga"},
                            )
                            emit(
                                "control.decision",
                                tick=tick,
                                flow_id=mutation.flow_id,
                                resource=resource,
                                payload={"decision": "block", "reason": "missing_saga"},
                            )
                            continue

                        if saga["status"] == "running":
                            saga["status"] = "committed"
                            emit(
                                "saga.committed",
                                tick=tick,
                                flow_id=mutation.flow_id,
                                resource=resource,
                                payload={"saga_id": saga_id},
                            )
                            emit(
                                "control.decision",
                                tick=tick,
                                flow_id=mutation.flow_id,
                                resource=resource,
                                payload={"decision": "allow", "reason": "saga_commit"},
                            )
                        else:
                            emit(
                                "saga.commit_skipped",
                                tick=tick,
                                flow_id=mutation.flow_id,
                                resource=resource,
                                payload={"saga_id": saga_id, "reason": f"status_{saga['status']}"},
                            )
                            emit(
                                "control.decision",
                                tick=tick,
                                flow_id=mutation.flow_id,
                                resource=resource,
                                payload={"decision": "block", "reason": f"status_{saga['status']}"},
                            )
                    continue

                if resource in state.circuits:
                    for blocked in contenders:
                        emit(
                            "flow.blocked_by_circuit",
                            tick=tick,
                            flow_id=blocked.flow_id,
                            resource=resource,
                            payload={"op_id": blocked.op_id, "kind": blocked.kind},
                        )
                        emit(
                            "control.decision",
                            tick=tick,
                            flow_id=blocked.flow_id,
                            resource=resource,
                            payload={"decision": "block", "reason": "circuit_open"},
                        )
                    continue

                winner = contenders[0]
                losers = contenders[1:]

                if winner.kind in {"task_upsert", "guarded_task"}:
                    state.tasks[winner.resource] = {
                        "task_id": winner.resource,
                        "owner_flow": winner.flow_id,
                        "title": str(winner.payload.get("title") or winner.resource),
                    }
                    emit(
                        "task.created",
                        tick=tick,
                        flow_id=winner.flow_id,
                        resource=winner.resource,
                        payload={"task_id": winner.resource, "title": state.tasks[winner.resource]["title"]},
                    )
                    emit(
                        "control.decision",
                        tick=tick,
                        flow_id=winner.flow_id,
                        resource=winner.resource,
                        payload={"decision": "allow", "reason": "task_winner"},
                    )
                    loser_event_type = "task.conflict_blocked"
                    loser_reason = "task_conflict"
                elif winner.kind == "slot_reserve":
                    state.slots[winner.resource] = {
                        "slot_id": winner.resource,
                        "owner_flow": winner.flow_id,
                    }
                    emit(
                        "slot.reserved",
                        tick=tick,
                        flow_id=winner.flow_id,
                        resource=winner.resource,
                        payload={"slot_id": winner.resource},
                    )
                    emit(
                        "control.decision",
                        tick=tick,
                        flow_id=winner.flow_id,
                        resource=winner.resource,
                        payload={"decision": "allow", "reason": "slot_winner"},
                    )
                    loser_event_type = "slot.conflict_blocked"
                    loser_reason = "slot_conflict"
                elif winner.kind == "saga_start":
                    saga_id = str(winner.payload.get("saga_id") or f"saga-{winner.flow_id}")
                    state.sagas[saga_id] = {
                        "saga_id": saga_id,
                        "flow_id": winner.flow_id,
                        "resource": winner.resource,
                        "status": "running",
                    }
                    emit(
                        "saga.started",
                        tick=tick,
                        flow_id=winner.flow_id,
                        resource=winner.resource,
                        payload={"saga_id": saga_id, "resource": winner.resource},
                    )
                    emit(
                        "control.decision",
                        tick=tick,
                        flow_id=winner.flow_id,
                        resource=winner.resource,
                        payload={"decision": "allow", "reason": "saga_winner"},
                    )
                    loser_event_type = "saga.compensated"
                    loser_reason = "saga_collision"
                else:
                    raise AssertionError(f"Unsupported mutation kind: {winner.kind}")

                for loser in losers:
                    if loser.kind == "saga_start":
                        loser_saga_id = str(loser.payload.get("saga_id") or f"saga-{loser.flow_id}")
                        state.sagas[loser_saga_id] = {
                            "saga_id": loser_saga_id,
                            "flow_id": loser.flow_id,
                            "resource": loser.resource,
                            "status": "compensated",
                        }
                        emit(
                            loser_event_type,
                            tick=tick,
                            flow_id=loser.flow_id,
                            resource=loser.resource,
                            payload={"saga_id": loser_saga_id, "reason": loser_reason},
                        )
                    else:
                        emit(
                            loser_event_type,
                            tick=tick,
                            flow_id=loser.flow_id,
                            resource=loser.resource,
                            payload={"op_id": loser.op_id, "reason": loser_reason},
                        )

                    emit(
                        "control.decision",
                        tick=tick,
                        flow_id=loser.flow_id,
                        resource=loser.resource,
                        payload={"decision": "block", "reason": loser_reason},
                    )

        runtime_output = self._output_from_runtime_state(state, events)
        replay_output = self._replay_output(events)
        api_response = {"projection": dict(runtime_output["projection"])}

        contract = validate_system_contract(
            event_stream=events,
            runtime_output=runtime_output,
            replay_output=replay_output,
            control_plane_decisions=runtime_output["control_plane_decisions"],
            api_response=api_response,
        )

        return {
            "event_stream": events,
            "runtime_output": runtime_output,
            "replay_output": replay_output,
            "api_response": api_response,
            "contract": contract,
        }

    def _output_from_runtime_state(self, state: SimulationState, events: list[dict[str, Any]]) -> dict[str, Any]:
        decisions = self._decisions_from_events(events)
        projection = {
            "tasks": dict(state.tasks),
            "slots": dict(state.slots),
            "sagas": dict(state.sagas),
            "circuits": sorted(state.circuits),
        }
        fsm_state = {
            "tasks": {key: {"current_state": "created", "owner_flow": value["owner_flow"]} for key, value in state.tasks.items()},
            "slots": {key: {"current_state": "reserved", "owner_flow": value["owner_flow"]} for key, value in state.slots.items()},
            "sagas": {
                key: {"current_state": str(value.get("status") or "unknown"), "flow_id": value.get("flow_id")}
                for key, value in state.sagas.items()
            },
        }
        return {
            "fsm_state": fsm_state,
            "projection": projection,
            "control_plane_decisions": decisions,
        }

    def _replay_output(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        replay_state = SimulationState(tasks={}, slots={}, sagas={}, circuits=set())

        for event in sorted(events, key=lambda item: int(item.get("sequence") or 0)):
            event_type = str(event.get("event_type") or "")
            resource = str(event.get("resource") or "")
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

            if event_type == "task.created":
                replay_state.tasks[resource] = {
                    "task_id": str(payload.get("task_id") or resource),
                    "owner_flow": str(event.get("flow_id") or ""),
                    "title": str(payload.get("title") or resource),
                }
            elif event_type == "slot.reserved":
                replay_state.slots[resource] = {
                    "slot_id": str(payload.get("slot_id") or resource),
                    "owner_flow": str(event.get("flow_id") or ""),
                }
            elif event_type == "saga.started":
                saga_id = str(payload.get("saga_id") or "")
                replay_state.sagas[saga_id] = {
                    "saga_id": saga_id,
                    "flow_id": str(event.get("flow_id") or ""),
                    "resource": resource,
                    "status": "running",
                }
            elif event_type == "saga.compensated":
                saga_id = str(payload.get("saga_id") or "")
                replay_state.sagas[saga_id] = {
                    "saga_id": saga_id,
                    "flow_id": str(event.get("flow_id") or ""),
                    "resource": resource,
                    "status": "compensated",
                }
            elif event_type == "saga.committed":
                saga_id = str(payload.get("saga_id") or "")
                existing = replay_state.sagas.setdefault(
                    saga_id,
                    {
                        "saga_id": saga_id,
                        "flow_id": str(event.get("flow_id") or ""),
                        "resource": resource,
                        "status": "running",
                    },
                )
                existing["status"] = "committed"
            elif event_type == "system.circuit_opened":
                replay_state.circuits.add(resource)

        return self._output_from_runtime_state(replay_state, events)

    @staticmethod
    def _decisions_from_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        decisions: list[dict[str, Any]] = []
        for event in sorted(events, key=lambda item: int(item.get("sequence") or 0)):
            if str(event.get("event_type") or "") != "control.decision":
                continue
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            decisions.append(
                {
                    "flow_id": str(event.get("flow_id") or ""),
                    "resource": str(event.get("resource") or ""),
                    "decision": str(payload.get("decision") or "unknown"),
                    "reason": str(payload.get("reason") or "unknown"),
                }
            )
        return decisions


def _collect_blocked_decisions(result: dict[str, Any]) -> list[dict[str, Any]]:
    decisions = result["runtime_output"]["control_plane_decisions"]
    return [item for item in decisions if item["decision"] == "block"]


@pytest.mark.ci_gate
def test_concurrent_flows_produce_consistent_state() -> None:
    harness = MultiFlowSimulationHarness()
    result = harness.run(
        [
            FlowMutation("flow-a", "op-1", 1, "task_upsert", "task-shared", {"title": "Task From A"}),
            FlowMutation("flow-b", "op-1", 1, "task_upsert", "task-shared", {"title": "Task From B"}),
            FlowMutation("flow-c", "op-1", 2, "slot_reserve", "slot-09:00", {}),
        ]
    )

    task_owner = result["runtime_output"]["projection"]["tasks"]["task-shared"]["owner_flow"]
    blocked = _collect_blocked_decisions(result)

    assert task_owner == "flow-a"
    assert any(item["reason"] == "task_conflict" for item in blocked)
    assert result["contract"] == {"matches": True, "differences": []}


@pytest.mark.migration
def test_conflict_resolution_is_deterministic() -> None:
    harness = MultiFlowSimulationHarness()

    shared_mutations = [
        FlowMutation("flow-a", "op-1", 1, "slot_reserve", "slot-10:00", {}),
        FlowMutation("flow-b", "op-1", 1, "slot_reserve", "slot-10:00", {}),
        FlowMutation("flow-c", "op-1", 1, "slot_reserve", "slot-10:00", {}),
        FlowMutation("flow-d", "op-1", 1, "slot_reserve", "slot-10:00", {}),
        FlowMutation("flow-e", "op-1", 1, "slot_reserve", "slot-10:00", {}),
    ]

    forward = harness.run(shared_mutations)
    reverse = harness.run(list(reversed(shared_mutations)))

    assert forward["runtime_output"] == reverse["runtime_output"]
    assert forward["replay_output"] == reverse["replay_output"]
    assert forward["event_stream"] == reverse["event_stream"]
    assert forward["runtime_output"]["projection"]["slots"]["slot-10:00"]["owner_flow"] == "flow-a"


@pytest.mark.ci_gate
def test_saga_collisions_resolve_correctly() -> None:
    harness = MultiFlowSimulationHarness()
    result = harness.run(
        [
            FlowMutation("flow-a", "start", 1, "saga_start", "entity:task-88", {"saga_id": "saga-a"}),
            FlowMutation("flow-b", "start", 1, "saga_start", "entity:task-88", {"saga_id": "saga-b"}),
            FlowMutation("flow-a", "commit", 2, "saga_commit", "entity:task-88", {"saga_id": "saga-a"}),
            FlowMutation("flow-b", "commit", 2, "saga_commit", "entity:task-88", {"saga_id": "saga-b"}),
        ]
    )

    sagas = result["runtime_output"]["projection"]["sagas"]
    blocked = _collect_blocked_decisions(result)

    assert sagas["saga-a"]["status"] == "committed"
    assert sagas["saga-b"]["status"] == "compensated"
    assert any(item["reason"] == "saga_collision" for item in blocked)
    assert result["contract"]["matches"] is True


@pytest.mark.reliability
def test_control_plane_blocks_conflicting_flows() -> None:
    harness = MultiFlowSimulationHarness()
    result = harness.run(
        [
            FlowMutation("flow-a", "open", 1, "circuit_open", "slot-11:00", {"reason": "error_burst"}),
            FlowMutation("flow-b", "reserve", 2, "slot_reserve", "slot-11:00", {}),
            FlowMutation("flow-c", "guarded", 2, "guarded_task", "slot-11:00", {"title": "Guarded"}),
            FlowMutation("flow-d", "safe", 2, "slot_reserve", "slot-11:30", {}),
        ]
    )

    blocked = _collect_blocked_decisions(result)
    slots = result["runtime_output"]["projection"]["slots"]

    assert any(item["resource"] == "slot-11:00" and item["reason"] == "circuit_open" for item in blocked)
    assert slots["slot-11:30"]["owner_flow"] == "flow-d"
    assert "slot-11:00" in result["runtime_output"]["projection"]["circuits"]


@pytest.mark.ci_gate
def test_multi_flow_replay_matches_runtime() -> None:
    harness = MultiFlowSimulationHarness()
    result = harness.run(
        [
            FlowMutation("flow-a", "task", 1, "task_upsert", "task-shared-2", {"title": "Task A"}),
            FlowMutation("flow-b", "task", 1, "task_upsert", "task-shared-2", {"title": "Task B"}),
            FlowMutation("flow-c", "slot", 1, "slot_reserve", "slot-12:00", {}),
            FlowMutation("flow-d", "saga", 2, "saga_start", "entity:slot-12:00", {"saga_id": "saga-d"}),
            FlowMutation("flow-e", "saga", 2, "saga_start", "entity:slot-12:00", {"saga_id": "saga-e"}),
            FlowMutation("flow-d", "commit", 3, "saga_commit", "entity:slot-12:00", {"saga_id": "saga-d"}),
            FlowMutation("flow-a", "open", 3, "circuit_open", "slot-12:30", {"reason": "risk_spike"}),
            FlowMutation("flow-b", "blocked", 4, "slot_reserve", "slot-12:30", {}),
        ]
    )

    assert result["runtime_output"] == result["replay_output"]
    assert result["contract"] == {"matches": True, "differences": []}
