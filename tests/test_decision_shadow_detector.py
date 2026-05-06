from __future__ import annotations

import pytest

from scripts.decision_card_shadow_detector import REPO_ROOT, collect_decision_shadow_violations


@pytest.mark.ci_gate
def test_decision_shadow_detector_reports_no_violations() -> None:
    violations = collect_decision_shadow_violations(REPO_ROOT)
    rendered = "\n".join(violation.render(REPO_ROOT) for violation in violations)
    assert not violations, f"Decision-card shadow detector violations found:\n{rendered}"
