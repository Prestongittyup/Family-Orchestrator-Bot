# STATUS: STUB MODULE — NOT PRODUCTION READY
from __future__ import annotations

from modules.core.models.module_output import ModuleOutput


def meal_module(household_id: str) -> ModuleOutput:
    """
    Generate meal proposals for a household.
    
    Currently returns empty ModuleOutput as meal plan table is not yet implemented.
    When meal plan table is available, this will query meal recommendations.
    """
    # TODO: Implement meal_plans table schema and queries
    # For now, return valid but empty ModuleOutput to maintain OS-2 contract
    
    return ModuleOutput(
        module="meal_module",
        proposals=[],
        signals=[],
        confidence=0.0,
        metadata={
            "household_id": household_id,
            "source": "stub_not_implemented",
            "note": "Meal module table not yet created",
        },
    )
