from __future__ import annotations

from fastapi import APIRouter, Query

from policy_engine.contracts import HouseholdMemorySnapshot, ItineraryResponse, PolicyRecomputeResponse, PolicySummaryResponse
from policy_engine.itinerary_generator import generate_daily_itinerary
from policy_engine.memory_store import PolicyMemoryStore
from policy_engine.policy_engine import PolicyRecommendationEngine


router = APIRouter(prefix="/policy", tags=["policy"])


@router.get("/summary", response_model=PolicySummaryResponse)
def get_policy_summary() -> PolicySummaryResponse:
    engine = PolicyRecommendationEngine()
    memory_snapshot = engine.memory_store.load_memory_snapshot() or engine.memory_store.build_memory_snapshot()
    return engine.build_policy_summary(memory_snapshot)


@router.get("/memory", response_model=HouseholdMemorySnapshot)
def get_policy_memory(
    household_id: str = Query(default="household-001"),
) -> HouseholdMemorySnapshot:
    store = PolicyMemoryStore()
    return store.load_memory_snapshot() or store.build_memory_snapshot(household_id=household_id)


@router.get("/itinerary", response_model=ItineraryResponse)
def get_policy_itinerary(
    household_id: str = Query(default="household-001"),
    target_date: str | None = Query(default=None),
) -> ItineraryResponse:
    store = PolicyMemoryStore()
    memory_snapshot = store.load_memory_snapshot() or store.build_memory_snapshot(household_id=household_id)
    return generate_daily_itinerary(memory_snapshot, target_date=target_date)


@router.post("/recompute", response_model=PolicyRecomputeResponse)
def recompute_policy_layer(
    household_id: str = Query(default="household-001"),
    target_date: str | None = Query(default=None),
) -> PolicyRecomputeResponse:
    engine = PolicyRecommendationEngine()
    memory_snapshot = engine.memory_store.build_memory_snapshot(household_id=household_id)
    persisted_snapshot = engine.memory_store.persist_memory_snapshot(memory_snapshot)
    policy_summary = engine.build_policy_summary(persisted_snapshot)
    itinerary = generate_daily_itinerary(persisted_snapshot, target_date=target_date)
    report = engine.build_behavior_summary(policy_summary, itinerary_date=itinerary.date)
    engine.memory_store.persist_policy_report(report)
    return PolicyRecomputeResponse(
        status="success",
        memory_snapshot=persisted_snapshot,
        policy_summary=policy_summary,
        itinerary=itinerary,
    )