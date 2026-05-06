from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from collections import OrderedDict
from typing import Any, Mapping, Sequence

from app.artifacts.action_artifact import ActionArtifact, build_action_artifact
from app.artifacts.coordination_artifacts import (
    ConflictArtifact,
    OverdueArtifact,
    UpcomingArtifact,
    build_conflicts,
    build_overdue,
    build_upcoming,
)
from app.artifacts.execution_plan_artifact import (
    ExecutionPlanArtifact,
    build_execution_plan_artifact,
)
from app.artifacts.priority_artifact import PriorityArtifact, build_priority_artifact
from app.artifacts.summary_artifact import SummaryArtifact, build_summary
from app.artifacts.today_view import TodayViewArtifact, build_today_view
from app.artifacts.validation_plan_artifact import (
    DEFAULT_VALIDATION_PLAN_VERSION,
    ValidationPlanArtifact,
    build_default_validation_plan,
)
from app.surfaces.household_loop_surface import (
    HouseholdLoopSurface,
    build_household_loop_surface,
)
from app.surfaces.household_decision_surface import (
    HouseholdDecisionSurface,
    build_household_decision_surface,
)
from app.surfaces.household_decision_feedback_surface import (
    HouseholdDecisionFeedbackSurface,
    build_household_decision_feedback_surface,
)
from app.surfaces.household_trajectory_surface import (
    HouseholdTrajectorySurface,
    build_household_trajectory_surface,
)
from app.surfaces.household_insight_surface import (
    HouseholdInsightSurface,
    build_household_insight_surface,
)


_MAX_TODAY_VIEW_CACHE_ENTRIES = 256
_MAX_CONFLICT_CACHE_ENTRIES = 256
_MAX_UPCOMING_CACHE_ENTRIES = 256
_MAX_OVERDUE_CACHE_ENTRIES = 256
_MAX_SUMMARY_CACHE_ENTRIES = 256
_MAX_PRIORITY_CACHE_ENTRIES = 256
_MAX_ACTION_CACHE_ENTRIES = 256
_MAX_EXECUTION_PLAN_CACHE_ENTRIES = 256
_MAX_VALIDATION_PLAN_CACHE_ENTRIES = 64
_MAX_HOUSEHOLD_LOOP_SURFACE_CACHE_ENTRIES = 256
_MAX_HOUSEHOLD_DECISION_SURFACE_CACHE_ENTRIES = 256
_MAX_HOUSEHOLD_DECISION_FEEDBACK_SURFACE_CACHE_ENTRIES = 256
_MAX_HOUSEHOLD_TRAJECTORY_SURFACE_CACHE_ENTRIES = 256
_MAX_HOUSEHOLD_INSIGHT_SURFACE_CACHE_ENTRIES = 256

_TODAY_VIEW_CACHE: OrderedDict[tuple[str, str, str], TodayViewArtifact] = OrderedDict()
_CONFLICT_CACHE: OrderedDict[tuple[str, str], ConflictArtifact] = OrderedDict()
_UPCOMING_CACHE: OrderedDict[tuple[str, str, str, str], UpcomingArtifact] = OrderedDict()
_OVERDUE_CACHE: OrderedDict[tuple[str, str], OverdueArtifact] = OrderedDict()
_SUMMARY_CACHE: OrderedDict[tuple[str, str, str], SummaryArtifact] = OrderedDict()
_PRIORITY_CACHE: OrderedDict[tuple[str, str, str], PriorityArtifact] = OrderedDict()
_ACTION_CACHE: OrderedDict[tuple[str, str, str], ActionArtifact] = OrderedDict()
_EXECUTION_PLAN_CACHE: OrderedDict[tuple[str, str, str], ExecutionPlanArtifact] = OrderedDict()
_VALIDATION_PLAN_CACHE: OrderedDict[tuple[str, str], ValidationPlanArtifact] = OrderedDict()
_HOUSEHOLD_LOOP_SURFACE_CACHE: OrderedDict[
    tuple[str, str, str],
    HouseholdLoopSurface,
] = OrderedDict()
_HOUSEHOLD_DECISION_SURFACE_CACHE: OrderedDict[
    tuple[str, str, str],
    HouseholdDecisionSurface,
] = OrderedDict()
_HOUSEHOLD_DECISION_FEEDBACK_SURFACE_CACHE: OrderedDict[
    tuple[str, str, str],
    HouseholdDecisionFeedbackSurface,
] = OrderedDict()
_HOUSEHOLD_TRAJECTORY_SURFACE_CACHE: OrderedDict[
    tuple[str, str, str, str],
    HouseholdTrajectorySurface,
] = OrderedDict()
_HOUSEHOLD_INSIGHT_SURFACE_CACHE: OrderedDict[
    tuple[str, str, str],
    HouseholdInsightSurface,
] = OrderedDict()


class _FrozenDict(dict[str, Any]):
    def __setitem__(self, key: Any, value: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def __delitem__(self, key: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def clear(self) -> None:
        raise TypeError("Artifact cache values are immutable")

    def pop(self, key: Any, default: Any = None) -> Any:
        raise TypeError("Artifact cache values are immutable")

    def popitem(self) -> tuple[Any, Any]:
        raise TypeError("Artifact cache values are immutable")

    def setdefault(self, key: Any, default: Any = None) -> Any:
        raise TypeError("Artifact cache values are immutable")

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("Artifact cache values are immutable")


class _FrozenList(list[Any]):
    def __setitem__(self, key: Any, value: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def __delitem__(self, key: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def append(self, value: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def clear(self) -> None:
        raise TypeError("Artifact cache values are immutable")

    def extend(self, values: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def insert(self, index: int, value: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def pop(self, index: int = -1) -> Any:
        raise TypeError("Artifact cache values are immutable")

    def remove(self, value: Any) -> None:
        raise TypeError("Artifact cache values are immutable")

    def reverse(self) -> None:
        raise TypeError("Artifact cache values are immutable")

    def sort(self, *args: Any, **kwargs: Any) -> None:
        raise TypeError("Artifact cache values are immutable")


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _FrozenDict({str(key): _freeze_value(nested) for key, nested in value.items()})
    if isinstance(value, list):
        return _FrozenList([_freeze_value(item) for item in value])
    return value


def _freeze_artifact(value: Mapping[str, Any]) -> dict[str, Any]:
    frozen = _freeze_value(value)
    if not isinstance(frozen, dict):
        raise TypeError("Artifact freeze failed")
    return frozen


def _cache_get[K, V](cache: OrderedDict[K, V], key: K) -> V | None:
    cached = cache.get(key)
    if cached is None:
        return None
    cache.move_to_end(key)
    return cached


def _cache_set[K, V](cache: OrderedDict[K, V], key: K, value: V, *, max_entries: int) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > max_entries:
        cache.popitem(last=False)


def get_today_view(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    projection: Mapping[str, Any],
) -> TodayViewArtifact:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_TODAY_VIEW_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_today_view(dict(projection), date)
    frozen = _freeze_artifact(built)
    _cache_set(
        _TODAY_VIEW_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_TODAY_VIEW_CACHE_ENTRIES,
    )
    return frozen


def get_conflicts(
    household_id: str,
    projection_version: str,
    *,
    projection: Mapping[str, Any],
) -> ConflictArtifact:
    cache_key = (household_id, projection_version)
    cached = _cache_get(_CONFLICT_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_conflicts(dict(projection))
    frozen = _freeze_artifact(built)
    _cache_set(
        _CONFLICT_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_CONFLICT_CACHE_ENTRIES,
    )
    return frozen


def get_upcoming(
    household_id: str,
    projection_version: str,
    window_start: str,
    window_end: str,
    *,
    projection: Mapping[str, Any],
    now: str,
    days: int,
) -> UpcomingArtifact:
    cache_key = (household_id, projection_version, window_start, window_end)
    cached = _cache_get(_UPCOMING_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_upcoming(dict(projection), now, days=days)
    frozen = _freeze_artifact(built)
    _cache_set(
        _UPCOMING_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_UPCOMING_CACHE_ENTRIES,
    )
    return frozen


def get_overdue(
    household_id: str,
    projection_version: str,
    *,
    projection: Mapping[str, Any],
    now: str,
) -> OverdueArtifact:
    cache_key = (household_id, projection_version)
    cached = _cache_get(_OVERDUE_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_overdue(dict(projection), now)
    frozen = _freeze_artifact(built)
    _cache_set(
        _OVERDUE_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_OVERDUE_CACHE_ENTRIES,
    )
    return frozen


def get_summary(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    today_view: Mapping[str, Any],
    conflicts: Mapping[str, Any],
    upcoming: Mapping[str, Any],
    overdue: Mapping[str, Any],
) -> SummaryArtifact:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_SUMMARY_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_summary(
        dict(today_view),
        dict(conflicts),
        dict(upcoming),
        dict(overdue),
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _SUMMARY_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_SUMMARY_CACHE_ENTRIES,
    )
    return frozen


def get_priority(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    summary: Mapping[str, Any],
    overdue: Mapping[str, Any],
    conflicts: Mapping[str, Any],
    today: Mapping[str, Any],
    upcoming: Mapping[str, Any],
) -> PriorityArtifact:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_PRIORITY_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_priority_artifact(
        dict(summary),
        dict(overdue),
        dict(conflicts),
        dict(today),
        dict(upcoming),
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _PRIORITY_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_PRIORITY_CACHE_ENTRIES,
    )
    return frozen


def get_actions(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    priority: Mapping[str, Any],
    summary: Mapping[str, Any],
    today: Mapping[str, Any],
) -> ActionArtifact:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_ACTION_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_action_artifact(
        dict(priority),
        dict(summary),
        dict(today),
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _ACTION_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_ACTION_CACHE_ENTRIES,
    )
    return frozen


def get_execution_plans(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    actions: Mapping[str, Any],
) -> ExecutionPlanArtifact:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_EXECUTION_PLAN_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_execution_plan_artifact(
        dict(actions),
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _EXECUTION_PLAN_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_EXECUTION_PLAN_CACHE_ENTRIES,
    )
    return frozen


def get_household_loop_surface(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    today_view: Mapping[str, Any],
    conflicts: Mapping[str, Any],
    upcoming: Mapping[str, Any],
    overdue: Mapping[str, Any],
    summary: Mapping[str, Any],
    priority: Mapping[str, Any],
    actions: Mapping[str, Any],
    execution_plans: Mapping[str, Any] | None = None,
) -> HouseholdLoopSurface:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_HOUSEHOLD_LOOP_SURFACE_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_household_loop_surface(
        dict(today_view),
        dict(conflicts),
        dict(upcoming),
        dict(overdue),
        dict(summary),
        dict(priority),
        dict(actions),
        dict(execution_plans) if isinstance(execution_plans, Mapping) else None,
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _HOUSEHOLD_LOOP_SURFACE_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_HOUSEHOLD_LOOP_SURFACE_CACHE_ENTRIES,
    )
    return frozen


def get_household_decision_surface(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    loop_surface: Mapping[str, Any],
) -> HouseholdDecisionSurface:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_HOUSEHOLD_DECISION_SURFACE_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_household_decision_surface(dict(loop_surface))
    frozen = _freeze_artifact(built)
    _cache_set(
        _HOUSEHOLD_DECISION_SURFACE_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_HOUSEHOLD_DECISION_SURFACE_CACHE_ENTRIES,
    )
    return frozen


def get_household_decision_feedback_surface(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    decision_surface: Mapping[str, Any],
    execution_plans: Mapping[str, Any],
    actions: Mapping[str, Any],
    pre_loop_surface: Mapping[str, Any],
    post_loop_surface: Mapping[str, Any],
) -> HouseholdDecisionFeedbackSurface:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_HOUSEHOLD_DECISION_FEEDBACK_SURFACE_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_household_decision_feedback_surface(
        dict(decision_surface),
        dict(execution_plans),
        dict(actions),
        dict(pre_loop_surface),
        dict(post_loop_surface),
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _HOUSEHOLD_DECISION_FEEDBACK_SURFACE_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_HOUSEHOLD_DECISION_FEEDBACK_SURFACE_CACHE_ENTRIES,
    )
    return frozen


def get_household_trajectory_surface(
    household_id: str,
    projection_version: str,
    start_date: str,
    end_date: str,
    *,
    feedback_surfaces: Sequence[Mapping[str, Any]],
) -> HouseholdTrajectorySurface:
    cache_key = (household_id, projection_version, start_date, end_date)
    cached = _cache_get(_HOUSEHOLD_TRAJECTORY_SURFACE_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_household_trajectory_surface(list(feedback_surfaces))
    frozen = _freeze_artifact(built)
    _cache_set(
        _HOUSEHOLD_TRAJECTORY_SURFACE_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_HOUSEHOLD_TRAJECTORY_SURFACE_CACHE_ENTRIES,
    )
    return frozen


def get_household_insight_surface(
    household_id: str,
    projection_version: str,
    date: str,
    *,
    trajectory_surface: Mapping[str, Any],
    feedback_surfaces: Sequence[Mapping[str, Any]],
    decision_surface: Mapping[str, Any],
) -> HouseholdInsightSurface:
    cache_key = (household_id, projection_version, date)
    cached = _cache_get(_HOUSEHOLD_INSIGHT_SURFACE_CACHE, cache_key)
    if cached is not None:
        return cached

    built = build_household_insight_surface(
        dict(trajectory_surface),
        list(feedback_surfaces),
        dict(decision_surface),
    )
    frozen = _freeze_artifact(built)
    _cache_set(
        _HOUSEHOLD_INSIGHT_SURFACE_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_HOUSEHOLD_INSIGHT_SURFACE_CACHE_ENTRIES,
    )
    return frozen


def get_validation_plan(
    household_id: str,
    validation_plan_version: str = DEFAULT_VALIDATION_PLAN_VERSION,
) -> ValidationPlanArtifact:
    cache_key = (household_id, validation_plan_version)
    cached = _cache_get(_VALIDATION_PLAN_CACHE, cache_key)
    if cached is not None:
        return cached

    if validation_plan_version != DEFAULT_VALIDATION_PLAN_VERSION:
        raise ValueError(f"Unsupported validation plan version: {validation_plan_version}")

    built = build_default_validation_plan(household_id=household_id)
    frozen = _freeze_artifact(built)
    _cache_set(
        _VALIDATION_PLAN_CACHE,
        cache_key,
        frozen,
        max_entries=_MAX_VALIDATION_PLAN_CACHE_ENTRIES,
    )
    return frozen
