from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict, Field

from apps.api.integration_core.models.household_state import HouseholdState
from household_os.runtime.orchestrator import HouseholdOSOrchestrator, RuntimeTickResult


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
            now=timestamp,
        )
        graph = self.orchestrator.state_store.load_graph(household_id)
        graph.setdefault("runtime", {}).setdefault("daily_cycle", {})["last_morning_run"] = self._iso(timestamp)
        self.orchestrator.state_store.save_graph(graph)
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
        graph = self.orchestrator.state_store.load_graph(household_id)
        queued_follow_ups = self.orchestrator.action_pipeline.queue_next_day_follow_ups(graph=graph, now=timestamp)
        graph.setdefault("runtime", {}).setdefault("daily_cycle", {})["last_evening_run"] = self._iso(timestamp)
        self.orchestrator.state_store.save_graph(graph)
        tick = self.orchestrator.tick(
            household_id=household_id,
            state=state,
            fitness_goal=fitness_goal,
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