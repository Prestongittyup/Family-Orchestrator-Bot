# run_home_flow.py

from app.projection import get_projection
from app.surfaces.household_home_screen_surface import build_household_home_screen_surface
from app.runtime import dispatch_command

HOUSEHOLD_ID = "test-house"
DATE = "2026-05-05"

def run():
    # 1. initial projection
    projection = get_projection(HOUSEHOLD_ID)

    # 2. build home screen
    before = build_household_home_screen_surface(
        household_id=HOUSEHOLD_ID,
        projection=projection,
        date=DATE
    )

    print("BEFORE:", before["primary_card"])

    # 3. execute command
    decision_id = before["primary_card"]["id"]

    dispatch_command({
        "type": "DecisionComplete",
        "household_id": HOUSEHOLD_ID,
        "decision_id": decision_id
    })

    # 4. rebuild projection
    projection = get_projection(HOUSEHOLD_ID)

    # 5. rebuild home screen
    after = build_household_home_screen_surface(
        household_id=HOUSEHOLD_ID,
        projection=projection,
        date=DATE
    )

    print("AFTER:", after["primary_card"])

if __name__ == "__main__":
    run()