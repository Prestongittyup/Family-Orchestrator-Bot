from __future__ import annotations

import ast
from pathlib import Path

import pytest

from household_os.core.lifecycle_state import LifecycleState, parse_lifecycle_state
from household_os.presentation.lifecycle_presentation_mapper import LifecyclePresentationMapper

API_FILES = [
    Path("apps/api/assistant_runtime_router.py"),
]
FORBIDDEN_DIRECT_LABELS = {
    LifecycleState.COMMITTED.value,
    LifecycleState.REJECTED.value,
    LifecycleState.FAILED.value,
    LifecycleState.APPROVED.value,
}


def _is_mapper_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    if not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr != "to_api_state":
        return False
    if not isinstance(node.func.value, ast.Name):
        return False
    return node.func.value.id == "LifecyclePresentationMapper"


def test_presentation_mapper_contract() -> None:
    assert LifecyclePresentationMapper.to_api_state(LifecycleState.COMMITTED) == "executed"
    assert LifecyclePresentationMapper.to_api_state(LifecycleState.FAILED) == "failed"
    assert LifecyclePresentationMapper.to_api_state(LifecycleState.REJECTED) == "rejected"
    assert LifecyclePresentationMapper.to_api_state(LifecycleState.APPROVED) == "approved"


def test_fsm_rejects_presentation_label() -> None:
    with pytest.raises(ValueError):
        parse_lifecycle_state("executed")


def test_api_layer_uses_mapper_for_lifecycle_output() -> None:
    findings: list[str] = []

    for path in API_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            for key_node, value_node in zip(node.keys, node.values):
                if not (isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)):
                    continue
                if key_node.value not in {"state", "status"}:
                    continue

                if _is_mapper_call(value_node):
                    continue

                if isinstance(value_node, ast.Attribute) and value_node.attr == "value":
                    findings.append(f"{path}:{getattr(value_node, 'lineno', '?')}:direct_enum_value")
                    continue

                if isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                    lowered = value_node.value.strip().lower()
                    if lowered in FORBIDDEN_DIRECT_LABELS or lowered == "executed":
                        findings.append(f"{path}:{getattr(value_node, 'lineno', '?')}:{lowered}")

            
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Name):
                continue
            if func.id not in {"AssistantApproveResponse", "AssistantRejectResponse"}:
                continue
            for keyword in node.keywords:
                if keyword.arg != "status":
                    continue
                if _is_mapper_call(keyword.value):
                    continue
                if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                    findings.append(f"{path}:{getattr(keyword.value, 'lineno', '?')}:{keyword.value.value}")

    assert not findings, "API lifecycle outputs bypass mapper:\n" + "\n".join(findings)
