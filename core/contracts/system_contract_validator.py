from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


def _normalize(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize(value[key]) for key in sorted(value.keys(), key=lambda item: str(item))}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize(item) for item in value]
    return value


def _extract_path(payload: Any, path: tuple[str, ...]) -> Any:
    current = payload
    for key in path:
        if not isinstance(current, Mapping):
            return None
        if key not in current:
            return None
        current = current[key]
    return current


def _extract_first(payload: Any, paths: tuple[tuple[str, ...], ...]) -> Any:
    for path in paths:
        extracted = _extract_path(payload, path)
        if extracted is not None:
            return extracted
    return None


def _stable_dump(value: Any) -> str:
    return json.dumps(_normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class SystemContractValidator:
    """Validate a cross-layer system contract against one canonical event interpretation."""

    _FSM_PATHS = (
        ("fsm_state",),
        ("state",),
        ("current_state",),
        ("derived_state", "fsm_state"),
    )

    _PROJECTION_PATHS = (
        ("projection",),
        ("derived_state",),
        ("state_projection",),
    )

    _REPLAY_DECISION_PATHS = (
        ("control_plane_decisions",),
        ("control_decisions",),
        ("replay_decisions",),
        ("decisions",),
        ("derived_state", "control_plane_decisions"),
        ("projection", "control_plane_decisions"),
    )

    _API_PROJECTION_PATHS = (
        ("projection",),
        ("derived_state",),
        ("state_projection",),
        ("response", "projection"),
    )

    def validate(
        self,
        *,
        event_stream: Sequence[Mapping[str, Any] | Any],
        runtime_output: Mapping[str, Any],
        replay_output: Mapping[str, Any],
        control_plane_decisions: Sequence[Mapping[str, Any] | Any],
        api_response: Mapping[str, Any],
    ) -> dict[str, Any]:
        differences: list[str] = []

        if not event_stream:
            differences.append("event_stream_empty")

        runtime_normalized = _normalize(runtime_output)
        replay_normalized = _normalize(replay_output)
        if runtime_normalized != replay_normalized:
            differences.append("structural_equality_mismatch:runtime_vs_replay")

        runtime_fsm = _extract_first(runtime_output, self._FSM_PATHS)
        replay_fsm = _extract_first(replay_output, self._FSM_PATHS)
        if _normalize(runtime_fsm) != _normalize(replay_fsm):
            differences.append("fsm_state_equality_mismatch")

        runtime_projection = _extract_first(runtime_output, self._PROJECTION_PATHS)
        replay_projection = _extract_first(replay_output, self._PROJECTION_PATHS)
        if _normalize(runtime_projection) != _normalize(replay_projection):
            differences.append("projection_equality_mismatch:runtime_vs_replay")

        replay_decisions = _extract_first(replay_output, self._REPLAY_DECISION_PATHS)
        if _normalize(control_plane_decisions) != _normalize(replay_decisions):
            differences.append("control_decision_parity_mismatch")

        api_projection = _extract_first(api_response, self._API_PROJECTION_PATHS)
        if _normalize(api_projection) != _normalize(replay_projection):
            differences.append("api_projection_truth_mismatch")

        # Deterministic digest is computed to guarantee stable comparison behavior on repeated calls.
        _stable_dump(
            {
                "event_stream": event_stream,
                "runtime_output": runtime_output,
                "replay_output": replay_output,
                "control_plane_decisions": control_plane_decisions,
                "api_response": api_response,
            }
        )

        return {
            "matches": len(differences) == 0,
            "differences": differences,
        }


def validate_system_contract(
    *,
    event_stream: Sequence[Mapping[str, Any] | Any],
    runtime_output: Mapping[str, Any],
    replay_output: Mapping[str, Any],
    control_plane_decisions: Sequence[Mapping[str, Any] | Any],
    api_response: Mapping[str, Any],
) -> dict[str, Any]:
    validator = SystemContractValidator()
    return validator.validate(
        event_stream=event_stream,
        runtime_output=runtime_output,
        replay_output=replay_output,
        control_plane_decisions=control_plane_decisions,
        api_response=api_response,
    )
