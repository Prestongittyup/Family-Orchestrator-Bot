"""
Temporal Intelligence Layer Verification Plan

STRICT PASS/FAIL test suite for TIL implementation.

GATE CONDITIONS (must ALL pass to complete):
  1. TIL module imports successfully, no external service dependencies
  2. Contract interface exists, contains no logic
  3. Singleton dependency behavior (same instance returned)
  4. Zero domain coupling (no imports from task/calendar/email services)
  5. System still runs unchanged

Run this script to validate TIL integration.
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path
from typing import Any

# Setup logging for clarity
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s | %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# TEST 1: Module Integrity
# ============================================================================

def test_til_module_imports() -> bool:
    """
    TEST 1.1: TIL module imports successfully.
    PASS: No ImportError, no syntax errors.
    """
    test_name = "TEST 1.1 - TIL Module Imports"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer
        logger.info(f"✔ {test_name} PASS")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_til_no_external_deps() -> bool:
    """
    TEST 1.2: TIL has no external service dependencies.
    PASS: Only imports stdlib (datetime), no domain/service imports.
    """
    test_name = "TEST 1.2 - TIL No External Dependencies"
    try:
        import ast
        
        til_path = Path("apps/api/services/temporal_intelligence_layer.py")
        with open(til_path) as f:
            tree = ast.parse(f.read())
        
        # Collect all imports
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                imports.append(node.module or "")
        
        # Forbidden domains
        forbidden = [
            "apps.api.models",
            "apps.api.services.task_service",
            "apps.api.services.calendar_service",
            "apps.api.services.email_service",
            "apps.api.core.event_bus",
            "apps.api.core.database",
        ]
        
        violations = [imp for imp in imports if any(fbid in imp for fbid in forbidden)]
        
        if violations:
            logger.error(f"✗ {test_name} FAIL: Found forbidden imports: {violations}")
            return False
        
        logger.info(f"✔ {test_name} PASS (imports: {[i for i in imports if i and i != '__future__']})")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_til_deterministic() -> bool:
    """
    TEST 1.3: TIL outputs are deterministic.
    PASS: Same inputs always produce same outputs (excluding time-based).
    """
    test_name = "TEST 1.3 - TIL Deterministic Outputs"
    try:
        from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer
        
        til = TemporalIntelligenceLayer()
        
        # Test determinism: multiple calls with same inputs
        result1 = til.estimate_duration("email_received", {})
        result2 = til.estimate_duration("email_received", {})
        
        if result1 != result2:
            logger.error(f"✗ {test_name} FAIL: Non-deterministic output for estimate_duration")
            return False
        
        # Test determinism: availability always True
        avail1 = til.check_availability("user1", "household1")
        avail2 = til.check_availability("user1", "household1")
        
        if avail1 != avail2:
            logger.error(f"✗ {test_name} FAIL: Non-deterministic output for check_availability")
            return False
        
        logger.info(f"✔ {test_name} PASS")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 2: Contract Separation
# ============================================================================

def test_contract_exists() -> bool:
    """
    TEST 2.1: Contract interface module exists.
    PASS: til_contract.py exists and imports without error.
    """
    test_name = "TEST 2.1 - Contract Interface Exists"
    try:
        from apps.api.services.til_contract import TILContract
        logger.info(f"✔ {test_name} PASS")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_contract_no_logic() -> bool:
    """
    TEST 2.2: Contract contains no business logic.
    PASS: Contract file contains only Protocol definition and docstrings.
    """
    test_name = "TEST 2.2 - Contract No Business Logic"
    try:
        import ast
        
        contract_path = Path("apps/api/services/til_contract.py")
        with open(contract_path) as f:
            source = f.read()
            tree = ast.parse(source)
        
        # Find the TILContract class
        til_contract_class = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "TILContract":
                til_contract_class = node
                break
        
        if not til_contract_class:
            logger.error(f"✗ {test_name} FAIL: TILContract class not found")
            return False
        
        # Check for function bodies (logic)
        for item in til_contract_class.body:
            if isinstance(item, ast.FunctionDef):
                # Functions should only contain docstring and ellipsis (...) or pass
                non_doc_body = [n for n in item.body if not (
                    isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)
                )]
                
                # Allow only ellipsis or pass
                if non_doc_body and not all(
                    isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant) and n.value.value == Ellipsis
                    for n in non_doc_body
                ):
                    logger.error(f"✗ {test_name} FAIL: Found logic in {item.name}")
                    return False
        
        logger.info(f"✔ {test_name} PASS (Protocol-only definition)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_contract_interface_shape() -> bool:
    """
    TEST 2.3: Contract defines required interface.
    PASS: TILContract has check_availability, suggest_time_slot, estimate_duration.
    """
    test_name = "TEST 2.3 - Contract Interface Shape"
    try:
        from apps.api.services.til_contract import TILContract
        import inspect
        
        required_methods = {
            "check_availability",
            "suggest_time_slot",
            "estimate_duration",
        }
        
        protocol_methods = {
            name for name, method in inspect.getmembers(TILContract, predicate=inspect.isfunction)
        }
        
        if not required_methods.issubset(protocol_methods):
            missing = required_methods - protocol_methods
            logger.error(f"✗ {test_name} FAIL: Missing methods: {missing}")
            return False
        
        logger.info(f"✔ {test_name} PASS (all 3 methods present)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 3: Singleton Dependency Behavior
# ============================================================================

def test_singleton_instance_exists() -> bool:
    """
    TEST 3.1: Shared dependencies module exists.
    PASS: shared_dependencies.py imports without error.
    """
    test_name = "TEST 3.1 - Shared Dependencies Module Exists"
    try:
        from apps.api.services.shared_dependencies import get_til
        logger.info(f"✔ {test_name} PASS")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_singleton_returns_same_instance() -> bool:
    """
    TEST 3.2: get_til() returns the same instance on multiple calls.
    PASS: id(get_til()) == id(get_til()).
    """
    test_name = "TEST 3.2 - Singleton Returns Same Instance"
    try:
        from apps.api.services.shared_dependencies import get_til
        
        instance1 = get_til()
        instance2 = get_til()
        instance3 = get_til()
        
        if not (id(instance1) == id(instance2) == id(instance3)):
            logger.error(f"✗ {test_name} FAIL: Multiple instances returned")
            return False
        
        logger.info(f"✔ {test_name} PASS (instance id: {id(instance1)})")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_no_per_request_instantiation() -> bool:
    """
    TEST 3.3: TIL instance is not created per request.
    PASS: Instance is created at module import time (module-level singleton).
    """
    test_name = "TEST 3.3 - No Per-Request Instantiation"
    try:
        import ast
        
        shared_deps_path = Path("apps/api/services/shared_dependencies.py")
        with open(shared_deps_path) as f:
            source = f.read()
            tree = ast.parse(source)
        
        # Check for module-level instantiation (not inside function)
        module_level_assigns = [
            node for node in tree.body
            if isinstance(node, ast.Assign)
        ]
        
        til_instantiation = None
        for assign in module_level_assigns:
            if any(
                isinstance(target, ast.Name) and "_temporal_intelligence_layer" in target.id
                for target in assign.targets
            ):
                til_instantiation = assign
                break
        
        if not til_instantiation:
            logger.error(f"✗ {test_name} FAIL: TIL not instantiated at module level")
            return False
        
        logger.info(f"✔ {test_name} PASS (singleton at module level)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 4: Isolation Check
# ============================================================================

def test_til_no_domain_coupling() -> bool:
    """
    TEST 4.1: TIL does NOT import from domain services.
    PASS: No imports from task_service, calendar_service, email_service, etc.
    """
    test_name = "TEST 4.1 - TIL No Domain Coupling"
    try:
        import ast
        
        til_path = Path("apps/api/services/temporal_intelligence_layer.py")
        with open(til_path) as f:
            tree = ast.parse(f.read())
        
        # Collect all imports
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.append(node.module)
        
        # Forbidden domain couplings
        forbidden_domains = [
            "task_service",
            "calendar_service",
            "email_service",
            "event_bus",
            "database",
        ]
        
        violations = [imp for imp in imports if any(fbid in imp for fbid in forbidden_domains)]
        
        if violations:
            logger.error(f"✗ {test_name} FAIL: Found domain coupling: {violations}")
            return False
        
        logger.info(f"✔ {test_name} PASS (isolated from domain services)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


def test_shared_deps_no_logic() -> bool:
    """
    TEST 4.2: shared_dependencies.py is pure wiring (no logic).
    PASS: Only contains instantiation and getter functions, no business logic.
    """
    test_name = "TEST 4.2 - Shared Dependencies Pure Wiring"
    try:
        import ast
        
        shared_deps_path = Path("apps/api/services/shared_dependencies.py")
        with open(shared_deps_path) as f:
            tree = ast.parse(f.read())
        
        # Check for logic-bearing constructs (loops, conditionals, complex logic)
        forbidden_patterns = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.For, ast.While, ast.AsyncFor)):
                forbidden_patterns.append("loop")
            elif isinstance(node, ast.If):
                forbidden_patterns.append("conditional")
            elif isinstance(node, ast.Try):
                forbidden_patterns.append("exception handling")
        
        if forbidden_patterns:
            logger.error(f"✗ {test_name} FAIL: Found logic patterns: {forbidden_patterns}")
            return False
        
        logger.info(f"✔ {test_name} PASS (pure wiring module)")
        return True
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: {e}")
        return False


# ============================================================================
# TEST 5: System Still Runs
# ============================================================================

def test_system_still_runs() -> bool:
    """
    TEST 5.1: API still starts without errors.
    PASS: main.py imports and startup logic succeeds (no runtime errors).
    """
    test_name = "TEST 5.1 - System Still Runs"
    try:
        # Import the main app to ensure startup logic works
        from apps.api import main
        
        logger.info(f"✔ {test_name} PASS (main.py imports successfully)")
        return True
    except ImportError as e:
        logger.error(f"✗ {test_name} FAIL: Import error in main: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ {test_name} FAIL: Runtime error in main: {e}")
        return False


# ============================================================================
# RUNNER
# ============================================================================

def run_all_tests() -> bool:
    """Run all verification tests and return overall PASS/FAIL."""
    print("\n" + "="*80)
    print("TEMPORAL INTELLIGENCE LAYER VERIFICATION PLAN")
    print("="*80 + "\n")
    
    # TEST 1: Module Integrity
    print("TEST 1: MODULE INTEGRITY")
    print("-" * 80)
    t1_1 = test_til_module_imports()
    t1_2 = test_til_no_external_deps()
    t1_3 = test_til_deterministic()
    test1_pass = all([t1_1, t1_2, t1_3])
    
    # TEST 2: Contract Separation
    print("\nTEST 2: CONTRACT SEPARATION")
    print("-" * 80)
    t2_1 = test_contract_exists()
    t2_2 = test_contract_no_logic()
    t2_3 = test_contract_interface_shape()
    test2_pass = all([t2_1, t2_2, t2_3])
    
    # TEST 3: Singleton Dependency Behavior
    print("\nTEST 3: SINGLETON DEPENDENCY BEHAVIOR")
    print("-" * 80)
    t3_1 = test_singleton_instance_exists()
    t3_2 = test_singleton_returns_same_instance()
    t3_3 = test_no_per_request_instantiation()
    test3_pass = all([t3_1, t3_2, t3_3])
    
    # TEST 4: Isolation Check
    print("\nTEST 4: ISOLATION CHECK")
    print("-" * 80)
    t4_1 = test_til_no_domain_coupling()
    t4_2 = test_shared_deps_no_logic()
    test4_pass = all([t4_1, t4_2])
    
    # TEST 5: System Still Runs
    print("\nTEST 5: SYSTEM STILL RUNS")
    print("-" * 80)
    t5_1 = test_system_still_runs()
    test5_pass = t5_1
    
    # Final gate check
    print("\n" + "="*80)
    all_pass = all([test1_pass, test2_pass, test3_pass, test4_pass, test5_pass])
    
    if all_pass:
        print("✔ OVERALL: PASS (all gates cleared)")
        print("="*80 + "\n")
        return True
    else:
        print("✗ OVERALL: FAIL (step is blocked)")
        print("="*80 + "\n")
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
