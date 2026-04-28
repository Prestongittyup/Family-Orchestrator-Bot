from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COZI_DASHBOARD = ROOT / "ui" / "src" / "components" / "live" / "CoziDashboard.jsx"


def _cozi_source() -> str:
    return COZI_DASHBOARD.read_text(encoding="utf-8")


def test_cozi_dashboard_exists() -> None:
    assert COZI_DASHBOARD.exists(), "Cozi dashboard component must exist for canonical wiring checks."


def test_cozi_fetches_brief_with_contract_validation() -> None:
    text = _cozi_source()

    assert "/brief/${householdId}?validate_contract_v1=true&include_observability=true" in text


def test_cozi_fetches_operational_context() -> None:
    text = _cozi_source()

    assert "/operational/context?household_id=" in text


def test_synthetic_overlay_is_explicitly_opt_in() -> None:
    text = _cozi_source()

    assert "SYNTHETIC_FALLBACK_ALLOWED" in text
    assert "get('synthetic') === '1'" in text
    assert "const syntheticEnabled = SYNTHETIC_FALLBACK_ALLOWED || syntheticQueryEnabled" in text
    assert "Synthetic fallback: {syntheticEnabled ? 'enabled' : 'disabled'}" in text
