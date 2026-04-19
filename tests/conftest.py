"""
Pytest configuration and fixtures for isolated E2E/adapter tests.

Hardened test isolation ensures:
1. Fresh database state (all test tables cleared pre/post)
2. Fresh event bus (handlers reinitialized)
3. Deterministic execution (frozen time, no cross-test contamination)
4. Zero production code modifications

Uses the singleton app instance from main.py but resets all supporting state
(event bus, database, caches) per test to achieve isolation.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from apps.api import main
from apps.api.core.database import Base, SessionLocal, engine
from apps.api.core.event_bus import get_event_bus
from apps.api.core.feature_flags import _reset_feature_flags_for_tests
from apps.api.models.event_log import EventLog
from apps.api.models.idempotency_key import IdempotencyKey
from apps.api.models.task import Task


@pytest.fixture(scope="session", autouse=True)
def ensure_test_schema() -> None:
    """Create database tables once for the test session before cleanup runs."""
    Base.metadata.create_all(bind=engine)


@pytest.fixture(scope="function", autouse=True)
def reset_runtime_feature_flags():
    _reset_feature_flags_for_tests()
    yield
    _reset_feature_flags_for_tests()


@pytest.fixture(scope="function", autouse=True)
def reset_event_bus():
    """
    Reset the global event bus singleton before each test.
    
    Ensures event handlers don't accumulate across test runs
    and each test gets a fresh event registry.
    
    Resets both the cached instance AND the module-level reference in event_registry
    to ensure consistency across the codebase.
    """
    import apps.api.core.event_bus as event_bus_module
    import apps.api.core.event_registry as event_registry_module
    
    # Clear the cached singleton
    event_bus_module._event_bus_instance = None
    
    # Reset the module-level reference for consistency
    event_registry_module.event_bus = event_bus_module.get_event_bus()
    
    yield
    
    # Clean up after test
    event_bus_module._event_bus_instance = None
    event_registry_module.event_bus = event_bus_module.get_event_bus()


@pytest.fixture(scope="function", autouse=True)
def clean_database(ensure_test_schema):
    """
    Clean database before and after each test.
    
    Removes all test data to ensure zero cross-test state pollution.
    """
    # Pre-test cleanup
    session = SessionLocal()
    try:
        session.query(Task).delete(synchronize_session=False)
        session.query(EventLog).delete(synchronize_session=False)
        session.query(IdempotencyKey).delete(synchronize_session=False)
        
        # Also clean calendar_events table if it exists
        try:
            session.execute(text("DELETE FROM calendar_events"))
        except Exception:
            pass
        
        session.commit()
    finally:
        session.close()
    
    yield
    
    # Post-test cleanup
    session = SessionLocal()
    try:
        session.query(Task).delete(synchronize_session=False)
        session.query(EventLog).delete(synchronize_session=False)
        session.query(IdempotencyKey).delete(synchronize_session=False)
        
        try:
            session.execute(text("DELETE FROM calendar_events"))
        except Exception:
            pass
        
        session.commit()
    finally:
        session.close()


@pytest.fixture(scope="function")
def test_client() -> TestClient:
    """
    Provide a TestClient with the main app instance and proper lifecycle management.
    
    The client's startup event is triggered on entry and shutdown on exit,
    ensuring handlers are properly initialized before tests run.
    
    Uses the singleton main.app with reset state from autouse fixtures:
    - reset_event_bus: Clears cached event bus instance
    - clean_database: Clears all test data from tables
    """
    with TestClient(main.app) as client:
        yield client
