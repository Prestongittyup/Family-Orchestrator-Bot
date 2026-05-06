from __future__ import annotations

import pytest

from scripts.decision_card_dependency_graph_guard import (
    REPO_ROOT,
    collect_dependency_graph_violations,
)


@pytest.mark.ci_gate
def test_decision_card_dependency_graph_guard_has_no_violations() -> None:
    violations = collect_dependency_graph_violations(REPO_ROOT)
    assert not violations, "\n".join(violations)
