from __future__ import annotations

from pathlib import Path

import pytest

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_SCREEN = ROOT / "hpal-frontend" / "src" / "ui" / "screens" / "DashboardScreen.tsx"


@pytest.mark.integration
def test_dashboard_decision_resolution_uses_persisted_backend_mutations() -> None:
    assert DASHBOARD_SCREEN.exists(), f"Missing dashboard screen file: {DASHBOARD_SCREEN}"
    source = DASHBOARD_SCREEN.read_text(encoding="utf-8")

    required_fragments = (
        "const onResolveDecisionOption = React.useCallback(",
        "await productSurfaceClient.completeDecision(",
        "await productSurfaceClient.deferDecision(",
        "await productSurfaceClient.ignoreDecision(",
        "const refreshedHome = await productSurfaceClient.fetchHomeV0(",
        "setHomeUxSurface(interpretHomeUxSurfaceContract(refreshedHome));",
        "const pendingDecisionCards = decisionCards;",
    )

    missing = [fragment for fragment in required_fragments if fragment not in source]
    assert missing == [], (
        "Dashboard decision flow must persist via canonical backend APIs and refresh from /home. "
        f"Missing fragments: {missing}"
    )

    forbidden_local_only_fragments = (
        "resolvedDecisionSelections",
        "setResolvedDecisionSelections",
        "decisionTransitionTick",
        "setDecisionTransitionTick",
    )

    present_local_only = [fragment for fragment in forbidden_local_only_fragments if fragment in source]
    assert present_local_only == [], (
        "Dashboard must not keep local-only decision resolution state that can diverge from backend truth. "
        f"Found fragments: {present_local_only}"
    )
