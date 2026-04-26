from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from household_os.core.lifecycle_state import LifecycleState, enforce_boundary_state


TriggerType = Literal[
    "USER_INPUT",
    "TIME_TICK",
    "STATE_CHANGE",
    "APPROVAL_PENDING_TIMEOUT",
]


class RuntimeTrigger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trigger_id: str
    trigger_type: TriggerType
    household_id: str
    detected_at: str
    detail: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class TriggerDetector:
    def detect(
        self,
        *,
        household_id: str,
        graph: dict[str, Any],
        user_input: str | None = None,
        now: str | datetime | None = None,
        pending_timeout_minutes: int | None = 720,
    ) -> list[RuntimeTrigger]:
        detected_at = self._coerce_datetime(now or graph.get("reference_time"))
        triggers: list[RuntimeTrigger] = []
        runtime = graph.get("runtime", {})

        if user_input:
            triggers.append(
                self._build_trigger(
                    household_id=household_id,
                    trigger_type="USER_INPUT",
                    detected_at=detected_at,
                    detail="User input requires orchestration",
                    metadata={"query": user_input},
                )
            )

        segment = self._time_segment(detected_at)
        last_time_tick = runtime.get("last_time_tick", {}) if isinstance(runtime, dict) else {}
        if segment is not None and last_time_tick.get(segment) != detected_at.date().isoformat():
            triggers.append(
                self._build_trigger(
                    household_id=household_id,
                    trigger_type="TIME_TICK",
                    detected_at=detected_at,
                    detail=f"{segment.title()} cycle trigger is due",
                    metadata={
                        "segment": segment,
                        "tick_key": f"{segment}:{detected_at.date().isoformat()}",
                    },
                )
            )

        last_processed_state_version = int(runtime.get("last_processed_state_version", 0)) if isinstance(runtime, dict) else 0
        current_state_version = int(graph.get("state_version", 0))
        if current_state_version > last_processed_state_version:
            triggers.append(
                self._build_trigger(
                    household_id=household_id,
                    trigger_type="STATE_CHANGE",
                    detected_at=detected_at,
                    detail="Household graph version advanced since the last orchestration tick",
                    metadata={
                        "previous_state_version": last_processed_state_version,
                        "current_state_version": current_state_version,
                    },
                )
            )

        lifecycle = graph.get("action_lifecycle", {})
        actions = lifecycle.get("actions", {}) if isinstance(lifecycle, dict) else {}
        timeout_override_seconds = (
            int(pending_timeout_minutes * 60)
            if pending_timeout_minutes is not None
            else None
        )
        for action in actions.values():
            current_state = enforce_boundary_state(action.get("current_state"))
            if current_state != LifecycleState.PENDING_APPROVAL:
                continue

            created_at_raw = str(action.get("created_at", ""))
            if not created_at_raw:
                continue

            created_at = self._coerce_datetime(created_at_raw)
            timeout_seconds = timeout_override_seconds if timeout_override_seconds is not None else int(12 * 60 * 60)
            if (detected_at - created_at).total_seconds() <= timeout_seconds:
                continue

            triggers.append(
                self._build_trigger(
                    household_id=household_id,
                    trigger_type="APPROVAL_PENDING_TIMEOUT",
                    detected_at=detected_at,
                    detail="Approval has remained pending beyond the timeout window",
                    metadata={
                        "action_id": str(action.get("action_id", "")),
                        "request_id": str(action.get("request_id", "")),
                        "age_minutes": int((detected_at - created_at).total_seconds() // 60),
                    },
                )
            )

        return triggers

    def _build_trigger(
        self,
        *,
        household_id: str,
        trigger_type: TriggerType,
        detected_at: datetime,
        detail: str,
        metadata: dict[str, Any],
    ) -> RuntimeTrigger:
        fingerprint = json.dumps(
            {
                "trigger_type": trigger_type,
                "household_id": household_id,
                "date": detected_at.isoformat(),
                "metadata": metadata,
            },
            sort_keys=True,
        )
        digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:12]
        return RuntimeTrigger(
            trigger_id=f"trg-{digest}",
            trigger_type=trigger_type,
            household_id=household_id,
            detected_at=detected_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            detail=detail,
            metadata=metadata,
        )

    def _coerce_datetime(self, value: str | datetime | None) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        if isinstance(value, str) and value:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        return datetime.now(UTC)

    def _time_segment(self, value: datetime) -> str | None:
        hour = value.hour
        if 5 <= hour < 12:
            return "morning"
        if 17 <= hour < 23:
            return "evening"
        return None