from __future__ import annotations

from dataclasses import dataclass

from apps.api.integration_core.models.household_state import HouseholdState
from apps.assistant_core.contracts import AssistantResponse
from assistant.contracts.assistant_plan import AssistantPlan as RuntimeAssistantPlan
from assistant.daily_loop.contracts import DailyPlan
from assistant.daily_loop.day_builder import DayBuilder
from assistant.runtime.assistant_runtime import AssistantRuntimeEngine


DEFAULT_DAILY_LOOP_QUERY = "Plan today with appointments, meals, and a workout around the family schedule"


@dataclass(frozen=True)
class DailyLoopResult:
    plan: DailyPlan
    approval_response: AssistantResponse | None = None


class DailyLoopEngine:
    def __init__(self) -> None:
        self._runtime_engine = AssistantRuntimeEngine()
        self._day_builder = DayBuilder()

    def build_from_runtime_plan(
        self,
        runtime_plan: RuntimeAssistantPlan,
        *,
        target_date: str | None = None,
        persisted: bool = False,
    ) -> DailyLoopResult:
        return DailyLoopResult(
            plan=self._day_builder.build(runtime_plan, target_date=target_date, persisted=persisted),
            approval_response=None,
        )

    def generate(
        self,
        *,
        query: str | None = None,
        household_id: str,
        repeat_window_days: int,
        fitness_goal: str | None,
        state: HouseholdState | None,
        runtime_plan: RuntimeAssistantPlan | None = None,
        target_date: str | None = None,
        persisted: bool = False,
    ) -> DailyLoopResult:
        if runtime_plan is not None:
            return self.build_from_runtime_plan(runtime_plan, target_date=target_date, persisted=persisted)

        runtime_result = self._runtime_engine.run(
            query=query or DEFAULT_DAILY_LOOP_QUERY,
            household_id=household_id,
            repeat_window_days=repeat_window_days,
            fitness_goal=fitness_goal,
            state=state,
        )
        return DailyLoopResult(
            plan=self._day_builder.build(runtime_result.plan, target_date=target_date, persisted=persisted),
            approval_response=runtime_result.approval_response,
        )