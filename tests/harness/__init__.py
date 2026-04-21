"""
HPAL Household Simulation & Failure Injection Test Harness

Validates real-world correctness of the orchestration system under
concurrent, failure-prone, distributed conditions.

Core Components:
  - SimulationEngine: Concurrent family member simulation
  - FailureInjector: Controlled fault injection
  - InvariantValidator: Hard constraints enforcement
  - EventLogger: Execution tracking for replay
  - StateHasher: Determinism verification
  - ScenarioRunner: End-to-end orchestration
  - ReportGenerator: Detailed failure analysis
"""

__version__ = "1.0.0"
