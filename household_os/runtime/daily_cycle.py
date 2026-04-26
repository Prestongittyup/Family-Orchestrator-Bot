from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from apps.api.integration_core.models.household_state import HouseholdState
from household_os.runtime.orchestrator import (
    HouseholdOSOrchestrator,
    OrchestratorRequest,
    RequestActionType,
    RuntimeTickResult,
)


class DailyCycleTickResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cycle: str
    tick: RuntimeTickResult
    queued_follow_ups: list[dict[str, str]] = Field(default_factory=list)


class HouseholdDailyCycle:
    def __init__(self, orchestrator: HouseholdOSOrchestrator | None = None) -> None:
        self.orchestrator = orchestrator or HouseholdOSOrchestrator()

    def run_morning(
        self,
        *,
        household_id: str,
        state: HouseholdState | None = None,
        fitness_goal: str | None = None,
        now: str | datetime | None = None,
    ) -> DailyCycleTickResult:
        timestamp = self._coerce_datetime(now or datetime.now(UTC).replace(hour=6, minute=30, second=0, microsecond=0))
        tick = self.orchestrator.tick(
            household_id=household_id,
            state=state,
            fitness_goal=fitness_goal,
            actor_type="system_worker",
            user_id="system",
            now=timestamp,
        )
        self.orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.UPDATE_DAILY_CYCLE_MARKER,
                household_id=household_id,
                actor={
                    "actor_type": "system_worker",
                    "subject_id": "system",
                    "session_id": None,
                    "verified": True,
                },
                cycle_marker="morning",
                cycle_timestamp=timestamp,
                context={"system_worker_verified": True},
            )
        )
        return DailyCycleTickResult(cycle="morning", tick=tick)

    def run_evening(
        self,
        *,
        household_id: str,
        state: HouseholdState | None = None,
        fitness_goal: str | None = None,
        now: str | datetime | None = None,
    ) -> DailyCycleTickResult:
        timestamp = self._coerce_datetime(now or datetime.now(UTC).replace(hour=19, minute=0, second=0, microsecond=0))
        queued_follow_ups = self.orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.QUEUE_FOLLOW_UPS,
                household_id=household_id,
                actor={
                    "actor_type": "system_worker",
                    "subject_id": "system",
                    "session_id": None,
                    "verified": True,
                },
                now=timestamp,
                context={"system_worker_verified": True},
            )
        )
        self.orchestrator.handle_request(
            OrchestratorRequest(
                action_type=RequestActionType.UPDATE_DAILY_CYCLE_MARKER,
                household_id=household_id,
                actor={
                    "actor_type": "system_worker",
                    "subject_id": "system",
                    "session_id": None,
                    "verified": True,
                },
                cycle_marker="evening",
                cycle_timestamp=timestamp,
                context={"system_worker_verified": True},
            )
        )
        tick = self.orchestrator.tick(
            household_id=household_id,
            state=state,
            fitness_goal=fitness_goal,
            actor_type="system_worker",
            user_id="system",
            now=timestamp,
        )
        return DailyCycleTickResult(cycle="evening", tick=tick, queued_follow_ups=queued_follow_ups)

    def _coerce_datetime(self, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)

    def _iso(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")