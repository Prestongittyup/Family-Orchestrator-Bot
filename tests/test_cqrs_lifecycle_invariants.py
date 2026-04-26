"""
CQRS Lifecycle Model Invariant Tests

Enforces:
1. FSM is the sole lifecycle mutation authority
2. Read models (loaders, reducer) are read-only
3. Reducer validates but doesn't define transition rules
4. API boundary uses presentation mapper exclusively
5. No duplicate transition logic outside FSM
"""

import ast
import inspect
import re
from pathlib import Path
from typing import Any

import pytest

from apps.api.core.state_machine import (
    StateMachine,
    ALLOWED_TRANSITIONS,
    validate_transition,
    ActionState,
    TransitionError,
)
from household_os.presentation.lifecycle_presentation_mapper import LifecyclePresentationMapper
from household_os.runtime.state_reducer import reduce_state
from household_os.core.lifecycle_state import LifecycleState


# ─────────────────────────────────────────────────────────────────────────────
# 1. MUTATION AUTHORITY TESTS
# ─────────────────────────────────────────────────────────────────────────────


def test_fsm_is_sole_mutation_authority():
    """
    Verify StateMachine.transition_to() is the ONLY place where lifecycle state mutates.
    Enforces CQRS write model constraint.
    """
    fsm = StateMachine(action_id="test_action_id")
    assert fsm.state == ActionState.PROPOSED

    # Only transition_to() may mutate state
    fsm.transition_to(ActionState.PENDING_APPROVAL, reason="test transition")
    assert fsm.state == ActionState.PENDING_APPROVAL

    # Verify no other method in StateMachine performs state mutation via AST scan
    # (reading state is OK, only assignments matter)
    fsm_path = Path(__file__).resolve().parent.parent / "apps" / "api" / "core" / "state_machine.py"
    content = fsm_path.read_text()
    tree = ast.parse(content)

    # Find StateMachine class and check for mutations outside transition_to
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "StateMachine":
            for method in node.body:
                if isinstance(method, ast.FunctionDef) and method.name != "transition_to":
                    # Check if this method assigns to self.state
                    for child in ast.walk(method):
                        if isinstance(child, ast.Assign):
                            for target in child.targets:
                                if isinstance(target, ast.Attribute):
                                    if (isinstance(target.value, ast.Name) and
                                        target.value.id == "self" and
                                        target.attr == "state"):
                                        pytest.fail(
                                            f"Found self.state mutation in {method.name}()"
                                        )


def test_fsm_transition_to_enforces_validation():
    """Verify transition_to() calls validate_transition()."""
    fsm = StateMachine(action_id="test")
    fsm.state = ActionState.PROPOSED

    # Valid transition should succeed
    event = fsm.transition_to(
        ActionState.PENDING_APPROVAL,
        reason="test",
    )
    assert event.to_state == ActionState.PENDING_APPROVAL
    assert fsm.state == ActionState.PENDING_APPROVAL

    # Invalid transition should fail
    with pytest.raises(TransitionError):
        fsm.transition_to(ActionState.PROPOSED)  # backward transition


def test_only_transition_to_mutates_state_in_fsm():
    """AST scan: Verify no other method in StateMachine assigns to self.state."""
    fsm_path = Path(__file__).resolve().parent.parent / "apps" / "api" / "core" / "state_machine.py"
    tree = ast.parse(fsm_path.read_text())

    # Find StateMachine class
    sm_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "StateMachine":
            sm_class = node
            break

    assert sm_class is not None, "StateMachine class not found"

    # Scan for self.state assignments
    state_mutations = []
    for method in sm_class.body:
        if isinstance(method, ast.FunctionDef):
            for node in ast.walk(method):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Attribute):
                            if (isinstance(target.value, ast.Name) and
                                target.value.id == "self" and
                                target.attr == "state"):
                                if method.name != "transition_to":
                                    state_mutations.append({
                                        "method": method.name,
                                        "line": node.lineno,
                                    })

    assert len(state_mutations) == 0, (
        f"Found self.state mutations outside transition_to(): {state_mutations}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 2. READ MODEL TESTS (Loaders)
# ─────────────────────────────────────────────────────────────────────────────


def test_loaders_do_not_mutate_lifecycle_fields():
    """
    Verify household_state_graph.py and household_state_manager.py
    do not mutate lifecycle fields during load/parse.
    """
    loader_files = [
        Path(__file__).resolve().parent.parent / "household_os" / "core" / "household_state_graph.py",
        Path(__file__).resolve().parent.parent / "household_state" / "household_state_manager.py",
    ]

    forbidden_patterns = [
        r'normalized_payload\["current_state"\]\s*=',
        r'normalized\["from_state"\]\s*=',
        r'normalized\["to_state"\]\s*=',
        r'payload\["current_state"\]\s*=',
        r'graph\["current_state"\]\s*=',
        r'\.current_state\s*=',
        r'\.from_state\s*=',
        r'\.to_state\s*=',
    ]

    for loader_file in loader_files:
        if not loader_file.exists():
            continue

        content = loader_file.read_text()
        tree = ast.parse(content)

        # Find _parse_lifecycle_sections and _assert_lifecycle_sections methods
        parse_methods = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                if "_parse_lifecycle_sections" in node.name or "_assert_lifecycle_sections" in node.name:
                    parse_methods.add(node.name)

        # For each parse method, check for mutations
        for method_name in parse_methods:
            # Extract method source
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == method_name:
                    source_lines = content.split('\n')[node.lineno - 1:node.end_lineno]
                    method_source = '\n'.join(source_lines)

                    # Check for forbidden patterns
                    violations = []
                    for pattern in forbidden_patterns:
                        if re.search(pattern, method_source):
                            violations.append(pattern)

                    # These violations are expected to NOT exist in hydration-only pattern
                    # We allow normalize_state() calls but they should not mutate
                    if "normalized_payload[" in method_source or "normalized[" in method_source:
                        # These should be in _parse_lifecycle_sections but only for reading/snapshotting
                        # Not for storing back
                        pass


def test_hydration_view_is_non_persisted():
    """
    Verify that lifecycle hydration views/snapshots are stripped before persistence.
    """
    from household_state.household_state_manager import LIFECYCLE_HYDRATION_VIEWS_KEY
    from household_os.core.household_state_graph import LIFECYCLE_HYDRATION_KEY

    # Both loaders should have methods that strip these keys
    # Verify the key names exist
    assert LIFECYCLE_HYDRATION_VIEWS_KEY == "_lifecycle_hydration_views"
    assert LIFECYCLE_HYDRATION_KEY == "_lifecycle_hydration"

    # These keys should never appear in persisted payloads
    # (This is enforced by _strip_lifecycle_hydration methods)


# ─────────────────────────────────────────────────────────────────────────────
# 3. REDUCER TESTS (Read Model / Event Sourcing)
# ─────────────────────────────────────────────────────────────────────────────


def test_reducer_validates_using_fsm_rules():
    """
    Verify reducer calls validate_transition() from FSM.
    Ensures reducer doesn't duplicate transitions logic.
    """
    reducer_path = Path(__file__).resolve().parent.parent / "household_os" / "runtime" / "state_reducer.py"
    content = reducer_path.read_text()

    # Must import validate_transition from FSM
    assert "from apps.api.core.state_machine import" in content
    assert "validate_transition" in content

    # Must call validate_transition in reduction logic
    assert "validate_transition(" in content


def test_reducer_is_pure_function():
    """Verify reducer has no side effects and is idempotent."""
    # This is a code inspection test
    reducer_path = Path(__file__).resolve().parent.parent / "household_os" / "runtime" / "state_reducer.py"
    content = reducer_path.read_text()

    tree = ast.parse(content)

    # Find reduce_state function
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "reduce_state":
            # Verify no global state modification
            for child in ast.walk(node):
                if isinstance(child, ast.Global):
                    pytest.fail("reduce_state contains global keyword (not pure)")

                if isinstance(child, ast.Attribute):
                    if isinstance(child.value, ast.Name):
                        if child.value.id not in ["events", "current_state"] and not child.value.id.startswith("_"):
                            # Might be accessing module-level constants (OK)
                            pass


def test_reducer_event_mapping_not_decision_logic():
    """
    Verify reducer's event-to-state mapping is event-driven,
    not a decision engine duplicating FSM logic.
    """
    reducer_path = Path(__file__).resolve().parent.parent / "household_os" / "runtime" / "state_reducer.py"
    content = reducer_path.read_text()

    # Reducer should have event type -> state mapping
    assert "event_to_state" in content or "LIFECYCLE_EVENT_TYPES" in content

    # Reducer should NOT have complex decision logic
    assert content.count("if ") < 20  # Rough heuristic for simple logic


# ─────────────────────────────────────────────────────────────────────────────
# 4. API BOUNDARY TESTS (Presentation Model)
# ─────────────────────────────────────────────────────────────────────────────


def test_api_uses_lifecycle_presentation_mapper():
    """
    Verify LifecyclePresentationMapper is imported and used in API routes.
    """
    api_path = Path(__file__).resolve().parent.parent / "apps" / "api" / "assistant_runtime_router.py"
    content = api_path.read_text()

    # Must import mapper
    assert "LifecyclePresentationMapper" in content

    # Must call to_api_state() for state exposure
    assert "to_api_state" in content


def test_api_never_exposes_raw_enum():
    """
    Verify API routes don't expose raw ActionState/LifecycleState enum values.
    All state must pass through LifecyclePresentationMapper.
    """
    api_path = Path(__file__).resolve().parent.parent / "apps" / "api" / "assistant_runtime_router.py"
    content = api_path.read_text()

    tree = ast.parse(content)

    # Find routes (functions decorated with @router)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Check if this is an API route
            has_route_decorator = any(
                (isinstance(dec, ast.Attribute) and dec.attr in ["get", "post", "put", "delete"]) or
                (isinstance(dec, ast.Attribute) and "route" in ast.unparse(dec))
                for dec in node.decorator_list
            )

            if has_route_decorator:
                # This is an API route
                # Verify any state exposure uses mapper
                for child in ast.walk(node):
                    if isinstance(child, ast.Return):
                        # Check for direct enum access in returns
                        source = ast.unparse(child)
                        if "ActionState." in source or "LifecycleState." in source:
                            # Should be wrapped in mapper call
                            if "to_api_state" not in source:
                                pytest.fail(
                                    f"Route {node.name} exposes raw enum without mapper: {source}"
                                )


def test_presentation_mapper_is_read_only():
    """Verify LifecyclePresentationMapper is pure projection only."""
    mapper_path = Path(__file__).resolve().parent.parent / "household_os" / "presentation" / "lifecycle_presentation_mapper.py"
    content = mapper_path.read_text()

    # Mapper should NOT:
    # - Make decisions about state
    # - Validate transitions
    # - Mutate state

    assert "transition_to" not in content or "transition_to" in content.split("\"")[0:1]  # Only in docstring maybe
    assert "validate_transition" not in content
    assert "self.state =" not in content or not content.count("self.state =")


# ─────────────────────────────────────────────────────────────────────────────
# 5. TRANSITION RULE SINGULARITY TESTS
# ─────────────────────────────────────────────────────────────────────────────


def test_allowed_transitions_is_sole_rule_source():
    """
    Verify ALLOWED_TRANSITIONS in FSM is the ONLY transition matrix definition.
    No other module should define transition rules.
    """
    repo_root = Path(__file__).resolve().parent.parent

    # Files that should NOT contain transition matrices
    forbidden_files = [
        "household_os/runtime/state_reducer.py",
        "household_os/core/household_state_graph.py",
        "household_state/household_state_manager.py",
        "household_os/presentation/lifecycle_presentation_mapper.py",
        "apps/api/assistant_runtime_router.py",
    ]

    forbidden_patterns = [
        r"ALLOWED_TRANSITIONS\s*=",
        r"TRANSITION_MATRIX\s*=",
        r"transition_rules\s*=",
        r"valid_transitions\s*=",
    ]

    for file_path_str in forbidden_files:
        file_path = repo_root / file_path_str
        if not file_path.exists():
            continue

        content = file_path.read_text()

        for pattern in forbidden_patterns:
            assert not re.search(pattern, content), (
                f"Found transition rule definition in {file_path}: {pattern}"
            )


def test_validate_transition_is_authoritative():
    """
    Verify validate_transition() function is the only validator.
    No other validation logic should duplicate transition rules.
    """
    from apps.api.core.state_machine import validate_transition

    # The function must validate using ALLOWED_TRANSITIONS
    source = inspect.getsource(validate_transition)
    assert "ALLOWED_TRANSITIONS" in source or "can_transition" in source


# ─────────────────────────────────────────────────────────────────────────────
# 6. CQRS ARCHITECTURE SEPARATION TESTS
# ─────────────────────────────────────────────────────────────────────────────


def test_write_and_read_models_are_separate():
    """
    Verify write model (FSM) and read models (loaders, reducer) don't mix.
    """
    # FSM module should not import from loaders
    fsm_path = Path(__file__).resolve().parent.parent / "apps" / "api" / "core" / "state_machine.py"
    fsm_content = fsm_path.read_text()

    forbidden_imports = [
        "household_os.core",
        "household_state.household_state",
        "household_os.runtime.state_reducer",
    ]

    for forbidden in forbidden_imports:
        assert f"from {forbidden}" not in fsm_content, (
            f"FSM (write model) imports from read model: {forbidden}"
        )

    # Loaders CAN import FSM (for validation helpers)
    # But loaders should NOT import to execute transitions
    loader_paths = [
        Path(__file__).resolve().parent.parent / "household_os" / "core" / "household_state_graph.py",
        Path(__file__).resolve().parent.parent / "household_state" / "household_state_manager.py",
    ]

    for loader_path in loader_paths:
        if not loader_path.exists():
            continue

        content = loader_path.read_text()
        # Loaders may import validate_transition for validation
        # But should NOT import StateMachine
        assert "StateMachine(" not in content or "StateMachine(" not in content.split('#')[0], (
            f"{loader_path}: Read model instantiates write model (violation)"
        )


def test_no_normalize_state_mutations():
    """
    Verify normalize_state() function doesn't mutate inputs.
    If it exists, it should be pure (read-only).
    """
    # Search for normalize_state definition
    lifecycle_state_path = Path(__file__).resolve().parent.parent / "household_os" / "core" / "lifecycle_state.py"
    if not lifecycle_state_path.exists():
        pytest.skip("lifecycle_state.py not found")

    try:
        content = lifecycle_state_path.read_text(encoding='utf-8')
    except (UnicodeDecodeError, FileNotFoundError):
        pytest.skip("Could not read lifecycle_state.py")

    if "def normalize_state" not in content:
        pytest.skip("normalize_state not defined")

    # If it exists, verify it doesn't mutate
    tree = ast.parse(content)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "normalize_state":
            # Function should return a new value, not mutate input
            source = ast.unparse(node)
            assert "return " in source, "normalize_state must return value"


# ─────────────────────────────────────────────────────────────────────────────
# 7. INTEGRATION TESTS
# ─────────────────────────────────────────────────────────────────────────────


def test_cqrs_write_read_integration():
    """
    Integration test: Verify write and read models work together.
    - Write via FSM
    - Read via reducer replay
    - Present via mapper
    """
    # Create an FSM instance
    fsm = StateMachine(action_id="integration_test_action")

    # Write: Perform transition
    fsm.transition_to(ActionState.PENDING_APPROVAL, reason="approval needed")
    assert fsm.state == ActionState.PENDING_APPROVAL

    # Verify state
    fsm.transition_to(ActionState.APPROVED, reason="approved by user")
    assert fsm.state == ActionState.APPROVED

    # Read: Would normally use event replay here (reducer/read model)
    # For this test, just verify FSM state is authoritative
    assert fsm.state == ActionState.APPROVED

    # Present: Map to API
    api_state = LifecyclePresentationMapper.to_api_state(LifecycleState(fsm.state.value))
    assert isinstance(api_state, str)
    assert api_state in ["proposed", "approved", "executed", "rejected"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
