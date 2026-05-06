from household_state.contracts import HouseholdDecisionResponse

__all__ = ["HouseholdDecisionEngine", "HouseholdDecisionResponse", "HouseholdStateManager"]


def __getattr__(name: str):
	if name == "HouseholdDecisionEngine":
		from household_state.decision_engine import HouseholdDecisionEngine

		return HouseholdDecisionEngine
	if name == "HouseholdStateManager":
		from household_state.household_state_manager import HouseholdStateManager

		return HouseholdStateManager
	raise AttributeError(f"module 'household_state' has no attribute {name!r}")