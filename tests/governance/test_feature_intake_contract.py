from __future__ import annotations

import ast
from pathlib import Path
import re
from typing import Any

import pytest

from tests.governance.contract_kernel import (
    FEATURE_INTAKE_ALLOWED_VALUES,
    FEATURE_INTAKE_DECLARATION_KEYS,
    FEATURE_INTAKE_DECLARATION_SYMBOL,
    FEATURE_INTAKE_GOVERNED_FILES,
    FEATURE_INTAKE_GOVERNED_PATTERNS,
    POLICY_DUPLICATE_PHRASES,
    PROJECTION_EVIDENCE_TOKENS,
    READ_MODEL_DETERMINISM_TOKENS,
    READ_MODEL_ENDPOINT_TOKENS,
)


_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate, pytest.mark.integration]


ROOT = Path(__file__).resolve().parents[2]

_RUNTIME_SCOPE_ROOTS = (
    ROOT / "app",
    ROOT / "apps",
    ROOT / "core",
    ROOT / "household_os",
    ROOT / "household_state",
)


def _read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def _feature_candidate_files() -> set[str]:
    candidates = set(FEATURE_INTAKE_GOVERNED_FILES)

    for pattern in FEATURE_INTAKE_GOVERNED_PATTERNS:
        for path in ROOT.glob(pattern):
            if path.is_file():
                candidates.add(path.relative_to(ROOT).as_posix())

    return candidates


def _declaration_for(relative_path: str) -> dict[str, str]:
    source = _read(relative_path)
    tree = ast.parse(source)

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == FEATURE_INTAKE_DECLARATION_SYMBOL:
                    payload = ast.literal_eval(node.value)
                    assert isinstance(payload, dict), (
                        f"{FEATURE_INTAKE_DECLARATION_SYMBOL} must be a dict in {relative_path}"
                    )
                    normalized = {str(key): str(value) for key, value in payload.items()}
                    return normalized

    raise AssertionError(f"Missing {FEATURE_INTAKE_DECLARATION_SYMBOL} in {relative_path}")


def _header_declaration_for(relative_path: str) -> dict[str, str]:
    source = _read(relative_path)

    header_pattern = re.compile(
        r"(?im)^\s*#\s*FEATURE_INTAKE:\s*$\n"
        r"^\s*#\s*projection_impact:\s*(yes|no)\s*$\n"
        r"^\s*#\s*read_model_impact:\s*(yes|no)\s*$\n"
        r"^\s*#\s*kernel_interaction:\s*(none|reference|extension)\s*$"
    )

    match = header_pattern.search(source)
    if not match:
        raise AssertionError(
            "Missing mandatory FEATURE_INTAKE header block in "
            f"{relative_path}. Expected lines: FEATURE_INTAKE + projection_impact/read_model_impact/kernel_interaction"
        )

    return {
        "projection_impact": str(match.group(1)).strip().lower(),
        "read_model_impact": str(match.group(2)).strip().lower(),
        "kernel_interaction": str(match.group(3)).strip().lower(),
    }


def test_feature_candidate_files_are_explicitly_governed() -> None:
    candidates = _feature_candidate_files()
    governed = set(FEATURE_INTAKE_GOVERNED_FILES)

    undeclared_candidates = sorted(candidates - governed)
    assert not undeclared_candidates, (
        "Feature-intake governance drift detected. Add new feature test files to "
        f"FEATURE_INTAKE_GOVERNED_FILES: {undeclared_candidates}"
    )

    for relative_path in sorted(governed):
        assert (ROOT / relative_path).exists(), f"Missing governed feature test file: {relative_path}"


def test_feature_files_define_complete_intake_declaration() -> None:
    for relative_path in FEATURE_INTAKE_GOVERNED_FILES:
        declaration = _declaration_for(relative_path)
        header_declaration = _header_declaration_for(relative_path)

        assert set(declaration.keys()) == set(FEATURE_INTAKE_DECLARATION_KEYS), (
            f"Invalid declaration keys in {relative_path}: {sorted(declaration.keys())}"
        )

        for key, allowed_values in FEATURE_INTAKE_ALLOWED_VALUES.items():
            assert declaration[key] in allowed_values, (
                f"Invalid {key} in {relative_path}: {declaration[key]}"
            )
            assert header_declaration[key] in allowed_values, (
                f"Invalid header {key} in {relative_path}: {header_declaration[key]}"
            )
            assert declaration[key] == header_declaration[key], (
                "Header/declaration mismatch in "
                f"{relative_path} for {key}: {header_declaration[key]} != {declaration[key]}"
            )


def test_feature_intake_template_remains_canonical() -> None:
    template_path = "tests/governance/feature_intake_declaration_template.py"
    declaration = _declaration_for(template_path)
    header_declaration = _header_declaration_for(template_path)

    assert set(declaration.keys()) == set(FEATURE_INTAKE_DECLARATION_KEYS), (
        f"Invalid declaration keys in {template_path}: {sorted(declaration.keys())}"
    )

    for key, allowed_values in FEATURE_INTAKE_ALLOWED_VALUES.items():
        assert declaration[key] in allowed_values, (
            f"Invalid {key} in {template_path}: {declaration[key]}"
        )
        assert header_declaration[key] in allowed_values, (
            f"Invalid header {key} in {template_path}: {header_declaration[key]}"
        )
        assert declaration[key] == header_declaration[key], (
            "Header/declaration mismatch in "
            f"{template_path} for {key}: {header_declaration[key]} != {declaration[key]}"
        )


def test_projection_only_features_do_not_couple_read_model_endpoints() -> None:
    for relative_path in FEATURE_INTAKE_GOVERNED_FILES:
        declaration = _declaration_for(relative_path)
        source = _read(relative_path)

        if declaration["projection_impact"] != "yes":
            continue
        if declaration["read_model_impact"] != "no":
            continue

        for token in READ_MODEL_ENDPOINT_TOKENS:
            assert token not in source, (
                f"Projection-only feature test leaked read-model endpoint token {token} in {relative_path}"
            )


def test_read_model_impact_features_assert_deterministic_projection_derived_behavior() -> None:
    for relative_path in FEATURE_INTAKE_GOVERNED_FILES:
        declaration = _declaration_for(relative_path)
        source = _read(relative_path)

        if declaration["read_model_impact"] != "yes":
            continue

        assert any(token in source for token in READ_MODEL_ENDPOINT_TOKENS), (
            f"Read-model-impact feature lacks endpoint assertions in {relative_path}"
        )
        assert any(token in source for token in READ_MODEL_DETERMINISM_TOKENS), (
            f"Read-model-impact feature lacks determinism/cache evidence in {relative_path}"
        )
        assert any(token in source for token in PROJECTION_EVIDENCE_TOKENS), (
            f"Read-model-impact feature lacks projection-derived evidence in {relative_path}"
        )


def test_kernel_extension_features_require_kernel_reference_without_duplication() -> None:
    for relative_path in FEATURE_INTAKE_GOVERNED_FILES:
        declaration = _declaration_for(relative_path)
        source = _read(relative_path)
        lowered = source.lower()

        if declaration["kernel_interaction"] == "none":
            continue

        assert (
            "tests.governance.contract_kernel" in source
            or "tests.governance.acs_reference" in source
        ), f"Kernel interaction missing kernel reference import in {relative_path}"

        if declaration["kernel_interaction"] == "extension":
            for phrase in POLICY_DUPLICATE_PHRASES:
                assert phrase not in lowered, (
                    f"Kernel extension duplicated policy phrase in {relative_path}: {phrase}"
                )


def test_feature_intake_enforcement_is_test_layer_only() -> None:
    forbidden_runtime_tokens = (
        FEATURE_INTAKE_DECLARATION_SYMBOL,
        "test_feature_intake_contract",
        "feature_intake_declaration",
    )

    violations: list[str] = []

    for scope_root in _RUNTIME_SCOPE_ROOTS:
        if not scope_root.exists():
            continue

        for path in scope_root.rglob("*.py"):
            source = path.read_text(encoding="utf-8")
            if any(token in source for token in forbidden_runtime_tokens):
                violations.append(path.relative_to(ROOT).as_posix())

    assert not violations, (
        "Feature intake enforcement leaked into runtime scopes: "
        f"{sorted(violations)}"
    )

    command_source = (ROOT / "app" / "api" / "command.py").read_text(encoding="utf-8")
    assert FEATURE_INTAKE_DECLARATION_SYMBOL not in command_source
    assert "feature_intake" not in command_source.lower()
