from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tests.evaluation.scenario_models import HouseholdScenario, ScenarioEvent


def _iso_at(day_offset: int, hour: int, minute: int = 0, duration_minutes: int = 60) -> tuple[str, str]:
    now = datetime.now(UTC)
    start = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=day_offset)
    end = start + timedelta(minutes=duration_minutes)
    return (
        start.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


def generate_scenarios() -> list[HouseholdScenario]:
    members = ["Alex", "Morgan", "Sam", "Jamie"]

    hc_school_start, hc_school_end = _iso_at(1, 8, 30, 45)
    hc_work_start, hc_work_end = _iso_at(1, 9, 0, 60)
    hc_doctor_start, hc_doctor_end = _iso_at(1, 9, 15, 45)

    la_walk_start, la_walk_end = _iso_at(1, 18, 0, 30)

    fo_breakfast_start, fo_breakfast_end = _iso_at(1, 7, 30, 45)
    fo_school_start, fo_school_end = _iso_at(1, 8, 0, 60)
    fo_review_start, fo_review_end = _iso_at(1, 8, 30, 45)
    fo_parent_call_start, fo_parent_call_end = _iso_at(1, 9, 0, 30)
    fo_demo_start, fo_demo_end = _iso_at(1, 9, 15, 90)
    fo_grocery_start, fo_grocery_end = _iso_at(1, 11, 0, 60)

    hp_doctor_start, hp_doctor_end = _iso_at(1, 10, 0, 90)
    hp_team_start, hp_team_end = _iso_at(1, 10, 15, 30)
    hp_lunch_start, hp_lunch_end = _iso_at(1, 12, 0, 45)

    bd_school_start, bd_school_end = _iso_at(1, 8, 0, 30)
    bd_work_start, bd_work_end = _iso_at(1, 13, 0, 45)
    bd_dinner_start, bd_dinner_end = _iso_at(1, 18, 30, 60)

    return [
        HouseholdScenario(
            scenario_id="high_conflict_day",
            description="Overlapping school drop-off, work sync, and doctor appointment.",
            household_members=members,
            events=[
                ScenarioEvent(
                    title="school_event",
                    start_time=hc_school_start,
                    end_time=hc_school_end,
                    participants=["Alex", "Sam"],
                    type="school",
                    priority_hint="high",
                ),
                ScenarioEvent(
                    title="work_meeting",
                    start_time=hc_work_start,
                    end_time=hc_work_end,
                    participants=["Alex"],
                    type="work",
                    priority_hint="high",
                ),
                ScenarioEvent(
                    title="doctor_appointment",
                    start_time=hc_doctor_start,
                    end_time=hc_doctor_end,
                    participants=["Morgan", "Sam"],
                    type="health",
                    priority_hint="critical",
                ),
            ],
            expected_signals={
                "must_surface": ["doctor_appointment", "school_event"],
                "required_titles": ["school_event", "work_meeting", "doctor_appointment"],
            },
            expected_outcomes={
                "top_priority": "doctor_appointment",
                "must_include": ["school_event", "work_meeting"],
                "must_flag_conflict": True,
                "must_not_include_noise": True,
            },
        ),
        HouseholdScenario(
            scenario_id="low_activity_day",
            description="Minimal events and low conflict schedule.",
            household_members=members,
            events=[
                ScenarioEvent(
                    title="evening_walk",
                    start_time=la_walk_start,
                    end_time=la_walk_end,
                    participants=["Jamie"],
                    type="personal",
                ),
            ],
            expected_signals={
                "must_surface": ["evening_walk"],
                "required_titles": ["evening_walk"],
            },
            expected_outcomes={
                "top_priority": "evening_walk",
                "must_include": ["evening_walk"],
                "must_flag_conflict": False,
                "must_not_include_noise": True,
            },
        ),
        HouseholdScenario(
            scenario_id="family_overload_day",
            description="Many competing priorities across school, work, and household responsibilities.",
            household_members=members,
            events=[
                ScenarioEvent("breakfast_prep", fo_breakfast_start, fo_breakfast_end, ["Jamie"], "personal"),
                ScenarioEvent("school_event", fo_school_start, fo_school_end, ["Sam"], "school", "high"),
                ScenarioEvent("design_review", fo_review_start, fo_review_end, ["Alex"], "work", "medium"),
                ScenarioEvent("parent_teacher_call", fo_parent_call_start, fo_parent_call_end, ["Morgan"], "school", "high"),
                ScenarioEvent("client_demo", fo_demo_start, fo_demo_end, ["Alex"], "work", "high"),
                ScenarioEvent("grocery_run", fo_grocery_start, fo_grocery_end, ["Jamie"], "personal", "medium"),
            ],
            expected_signals={
                "must_surface": ["school_event", "client_demo", "parent_teacher_call"],
                "required_titles": [
                    "breakfast_prep",
                    "school_event",
                    "design_review",
                    "parent_teacher_call",
                    "client_demo",
                    "grocery_run",
                ],
            },
            expected_outcomes={
                "top_priority": "client_demo",
                "must_include": ["school_event", "parent_teacher_call", "client_demo"],
                "must_flag_conflict": True,
                "must_not_include_noise": True,
            },
        ),
        HouseholdScenario(
            scenario_id="health_priority_day",
            description="Medical event should dominate over routine tasks.",
            household_members=members,
            events=[
                ScenarioEvent("doctor_appointment", hp_doctor_start, hp_doctor_end, ["Morgan"], "health", "critical"),
                ScenarioEvent("team_checkin", hp_team_start, hp_team_end, ["Alex"], "work", "medium"),
                ScenarioEvent("lunch_pickup", hp_lunch_start, hp_lunch_end, ["Jamie"], "personal", "low"),
            ],
            expected_signals={
                "must_surface": ["doctor_appointment"],
                "required_titles": ["doctor_appointment", "team_checkin", "lunch_pickup"],
            },
            expected_outcomes={
                "top_priority": "doctor_appointment",
                "must_include": ["team_checkin", "lunch_pickup"],
                "must_flag_conflict": True,
                "must_not_include_noise": True,
            },
        ),
        HouseholdScenario(
            scenario_id="balanced_day",
            description="Balanced distribution of school, work, and personal events.",
            household_members=members,
            events=[
                ScenarioEvent("school_event", bd_school_start, bd_school_end, ["Sam"], "school", "medium"),
                ScenarioEvent("work_meeting", bd_work_start, bd_work_end, ["Alex"], "work", "medium"),
                ScenarioEvent("family_dinner", bd_dinner_start, bd_dinner_end, ["Alex", "Morgan", "Sam", "Jamie"], "personal", "medium"),
            ],
            expected_signals={
                "must_surface": ["school_event", "work_meeting", "family_dinner"],
                "required_titles": ["school_event", "work_meeting", "family_dinner"],
            },
            expected_outcomes={
                "top_priority": "school_event",
                "must_include": ["work_meeting", "family_dinner"],
                "must_flag_conflict": False,
                "must_not_include_noise": True,
            },
        ),
    ]
