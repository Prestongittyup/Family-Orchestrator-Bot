# HPAL Household Simulation & Failure Injection Test Harness

## Overview

The Household Simulation & Failure Injection Test Harness is a comprehensive validation framework for the HPAL (Household Planning and Logistics) orchestration system. It enables rigorous testing under realistic, failure-prone, distributed conditions—validating that the system maintains data integrity, prevents duplicate executions, and achieves consistent convergence across concurrent operations.

## Key Components

### 1. **Simulation Engine** (`simulation_engine.py`)
Simulates realistic household operations with concurrent family members and coordinated task/plan management.

**Key Classes:**
- `SimulatedEntity`: Represents plans, tasks, or events with version tracking and watermark epoch support
- `FamilyMember`: Actor in the household with pending commands and entity context
- `SimulationEngine`: Orchestrates concurrent command execution with idempotency enforcement

**Key Features:**
- Concurrent family member execution
- Idempotency cache prevents duplicate task execution
- Watermark epoch tracking for version consistency
- Automatic quarantine mode on invariant violation
- Full event logging and state mutation audit trail

**Example:**
```python
engine = SimulationEngine()
member1 = engine.add_family_member("person_1", "family_1", "admin")
member2 = engine.add_family_member("person_2", "family_1", "member")

# Execute commands with failure injection
await engine.execute_command(command, failure_injector=injector)

# Check final state with state hash
final_hash = engine.state.state_hash()
```

### 2. **Failure Injector** (`failure_injector.py`)
Controlled fault injection to simulate real-world distributed system failures.

**Failure Modes:**
- `ORCHESTRATOR_CRASH`: Simulates orchestrator restart/recovery
- `PARTIAL_PERSISTENCE`: Incomplete state save (recovery scenario)
- `DELAYED_RESPONSE`: Network latency and timeouts
- `DUPLICATE_EXECUTION`: Idempotency test (command executed twice)
- `LEASE_EXPIRATION`: Leadership transition and lease renewal
- `NETWORK_TIMEOUT`: Connection failure and retry
- `RATE_LIMITING`: Service throttling (backoff required)
- `TRANSIENT_ERROR`: Temporary service unavailability
- `OUT_OF_ORDER_DELIVERY`: Message reordering

**Failure Profiles:**
1. **no_failures**: Baseline (0% injection rate)
2. **light_transient**: 5% transient, 10% delayed (network hiccups)
3. **moderate_network**: 10% timeout, 15% delayed, 5% duplicate (realistic network)
4. **high_chaos**: 5-15% per mode with cascade (stress test)
5. **byzantine**: 10-20% per mode without backoff (adversarial)

**Key Features:**
- Deterministic injection (seeded randomness for replay)
- Exponential backoff with configurable curves
- Cascade failures (e.g., crash→partial persistence)
- Probabilistic retry counting

**Example:**
```python
injector = FailureInjector(FailureInjectionProfile.moderate_network())

# Decide if failure should be injected
should_fail, mode, message = await injector.should_inject_failure(command)
if should_fail:
    retry_delay = injector.get_retry_delay(command.retry_count)
    await asyncio.sleep(retry_delay / 1000)
```

### 3. **Invariant Validator** (`invariant_validator.py`)
Enforces 8 hard constraints on system state—critical for detecting silent corruptions.

**Invariants (all CRITICAL severity):**
1. **No Duplicate Task Execution**: `task_execution_count[task_id] ≤ 1`
2. **No Lost Updates**: Entity count doesn't decrease unexpectedly
3. **No Cross-Family Leakage**: All entities belong to target `family_id`
4. **No Phantom States**: Only valid entity types and state transitions
5. **Version Monotonicity**: `entity.version` never decrements
6. **Timestamp Causality**: `updated_at ≥ created_at` for all entities
7. **Watermark Consistency**: `watermark_epoch ≥ 0`, no regression
8. **Quarantine Mode Safety**: Only set when invariant violation detected (WARNING)

**Example:**
```python
validator = InvariantValidator()
all_passed, violations = validator.run_all_validations(
    state=engine.state,
    family_id="family_1"
)

if not all_passed:
    for violation in violations:
        print(f"[{violation.severity}] {violation.invariant_name}")
        print(f"  {violation.description}")
```

### 4. **Scenario Runner** (`scenario_runner.py`)
Orchestrates end-to-end simulation runs with deterministic replay and convergence validation.

**Pre-defined Scenarios:**

1. **concurrent_plan_creation_scenario** (3 members, 5 plans each)
   - Tests: No lost updates, entity count correctness, watermark consistency
   - Validates: All created plans persisted despite concurrent access

2. **task_execution_idempotency_scenario** (2 members, 10 tasks each)
   - Tests: Duplicate execution prevention, idempotency cache
   - Validates: Same idempotency_key yields same result, execution count ≤ 1

3. **conflicting_plan_updates_scenario** (5 conflicting updates)
   - Tests: Concurrent modifications to same plan by different members
   - Validates: Version monotonicity preserved, no lost updates

**Key Methods:**
```python
runner = ScenarioRunner()

# Single scenario run
result = await runner.run_scenario("concurrent_plan_creation_scenario")

# Deterministic replay (same seed → same outcome)
converged, state_hashes = await runner.run_scenario_with_replay(
    "concurrent_plan_creation_scenario",
    num_replays=3
)

# Full test matrix (all scenarios × all failure profiles)
results = await runner.run_test_matrix()
summary = runner.get_results_summary()
```

### 5. **Report Generator** (`report_generator.py`)
Generates comprehensive analysis and reporting in JSON and human-readable formats.

**Report Types:**

1. **Event Log Report** (`{run_id}_event_log.json`)
   - Chronological simulation events
   - Command execution sequence
   - State mutations

2. **Failure Timeline Report** (`{run_id}_failure_timeline.json`)
   - Injected failures and recovery patterns
   - Command success/failure rates
   - Retry statistics

3. **Invariant Violation Report** (`{run_id}_invariant_violations.json`)
   - Detected violations with severity levels
   - Affected entities
   - Violation details for debugging

4. **State Hash Comparison Report** (`state_hash_comparison.json`)
   - Deterministic convergence analysis
   - Replay verification (identical runs → identical final state)
   - Hash consistency across replays

5. **Comprehensive Test Report** (`comprehensive_test_report.json`)
   - Full test matrix results
   - Aggregate statistics
   - Pass/fail classification per scenario

6. **Human-Readable Summary** (`test_summary.txt`)
   - Plain text format for easy reading
   - Success rate, violation counts, timing
   - Scenario-by-scenario breakdown

**Example:**
```python
generator = ReportGenerator(output_dir=Path("test_reports"))

# Generate individual reports
generator.generate_event_log_report(result)
generator.generate_invariant_violation_report(result)
generator.generate_state_hash_comparison_report(convergence_data)

# Generate comprehensive report
generator.generate_comprehensive_report(all_results, summary=summary)
generator.generate_human_readable_summary(all_results, summary=summary)
```

### 6. **CLI Interface** (`cli.py`)
Command-line runner for scenario execution, configuration, and report generation.

**Commands:**

```bash
# List available scenarios
python -m tests.harness list-scenarios

# List failure profiles
python -m tests.harness list-profiles

# Run single scenario (no failures baseline)
python -m tests.harness run-scenario \
  --scenario concurrent_plan_creation \
  --profile none \
  --format both

# Run scenario with network chaos
python -m tests.harness run-scenario \
  --scenario task_idempotency \
  --profile moderate

# Run full test matrix
python -m tests.harness run-matrix

# Verify deterministic replay (5 identical runs → same state hash)
python -m tests.harness run-replay \
  --scenario concurrent_plan_creation \
  --replays 5
```

### 7. **Integration Test Suite** (`test_household_simulation.py`)
Pytest-based test runner with automated report generation.

**Test Categories:**

1. **Invariant Validation Tests** (8 tests)
   - `test_no_duplicate_task_execution()`
   - `test_no_lost_updates()`
   - `test_no_cross_family_leakage()`
   - `test_no_phantom_states()`

2. **Failure Resilience Tests** (3 tests)
   - `test_resilience_to_transient_failures()`
   - `test_resilience_to_network_chaos()`
   - `test_resilience_to_byzantine_failures()`

3. **Deterministic Replay Tests** (1 test)
   - `test_deterministic_replay_convergence()`

4. **Test Matrix Tests** (1 test)
   - `test_full_matrix_execution()`

5. **Convergence Validation Tests** (2 tests)
   - `test_convergence_to_stable_state()`
   - `test_no_silent_inconsistencies()`

**Run Tests:**
```bash
# Run all tests
pytest tests/test_household_simulation.py -v

# Run specific test category
pytest tests/test_household_simulation.py -k "invariant" -v

# Run with detailed output
pytest tests/test_household_simulation.py -vvs

# Generate coverage report
pytest tests/test_household_simulation.py --cov=tests.harness
```

## Success Criteria

The harness validates the 6 explicit user requirements:

### 1. **Simulation Layer** ✓
- Multiple concurrent family members executing parallel HPAL commands
- Command queue with conflict detection
- Idempotency replay capability

### 2. **Failure Injection** ✓
- Orchestrator crash, partial persistence, network chaos, duplicate execution, lease expiration
- 5 failure profiles from benign to byzantine
- Deterministic failure injection for reproducibility

### 3. **Validation Rules** ✓
- 8 hard invariants enforced atomically
- No duplicate task execution, no lost updates, no cross-family leakage
- No phantom states, version monotonicity, timestamp causality
- Watermark consistency, quarantine mode safety

### 4. **Deterministic Replay** ✓
- Full replay capability with seeded randomness
- State hash comparison across identical runs
- Convergence validation (all replays → same final state)

### 5. **Output Requirements** ✓
- Event log, failure timeline, invariant violation report
- State hash comparison, pass/fail classification
- JSON and human-readable formats

### 6. **Success Criteria** ✓
- All runs converge to stable/quarantine state
- No silent inconsistencies (all violations detected)
- Deterministic failure resolution (replay verifies this)
- No cross-tenant contamination (family isolation enforced)

## Quick Start

### Installation

```bash
# Ensure test dependencies installed
pip install pytest pytest-asyncio

# Optional: for more detailed reporting
pip install pytest-html
```

### Running Tests

```bash
# Baseline scenario (no failures)
python -m tests.harness run-scenario --scenario concurrent_plan_creation --profile none

# Realistic network conditions
python -m tests.harness run-scenario --scenario task_idempotency --profile moderate

# Comprehensive test matrix
python -m tests.harness run-matrix

# Pytest integration
pytest tests/test_household_simulation.py -v
```

### Output Location

Reports are generated in `test_reports/` directory:
- `*_event_log.json` - Simulation events
- `*_failure_timeline.json` - Failure injections
- `*_invariant_violations.json` - Detected violations
- `state_hash_comparison.json` - Convergence analysis
- `comprehensive_test_report.json` - Full matrix results
- `test_summary.txt` - Human-readable summary
- `failure_classification.json` - Pass/fail categorization

## Architecture Diagrams

### Simulation Runtime
```
FamilyMember(person_1)
    ↓ (issue command)
SimulationEngine
    ↓
FailureInjector (decide if fail)
    ├─ PASS → execute command
    └─ FAIL → inject failure, retry with backoff
        ↓
    Idempotency Cache (check duplicate)
        ├─ HIT → return cached result
        └─ MISS → execute, cache result
            ↓
    HouseholdSimulationState (mutate)
        ├─ Update entity, increment version
        ├─ Log event
        └─ Update watermark_epoch
            ↓
    Command Result (success/failure)
        ↓
InvariantValidator
    └─ Check 8 constraints
        ├─ All pass → state safe
        └─ Any fail → quarantine_mode=true
```

### Test Execution Flow
```
ScenarioRunner.run_test_matrix()
    ├─ For each scenario:
    │   ├─ For each failure profile:
    │   │   ├─ Create SimulationEngine
    │   │   ├─ Add FamilyMembers
    │   │   ├─ Execute scenario generator
    │   │   ├─ Run InvariantValidator
    │   │   ├─ Collect result
    │   │   └─ Generate reports
    │   └─ Next profile
    └─ Aggregate results
        ├─ Success rate
        ├─ Violation summary
        └─ Timing metrics
```

## Troubleshooting

### Common Issues

1. **Imports fail**
   - Ensure `tests/harness/` directory structure is correct
   - Check `__init__.py` files exist and are not empty

2. **Async tests fail**
   - Install `pytest-asyncio`: `pip install pytest-asyncio`
   - Use `@pytest.mark.asyncio` decorator

3. **Reports not generated**
   - Check `test_reports/` directory is writable
   - Ensure ReportGenerator output_dir is set correctly

4. **Deterministic replay diverges**
   - Verify all randomness uses seeded `random.seed()`
   - Check FailureInjector.deterministic flag is True
   - Ensure scenario generator is pure (no external state)

## Performance Tuning

### Scenario Parameters
- Reduce `num_members` or `tasks_per_member` for faster runs
- Increase for more thorough testing and edge case detection

### Failure Injection Probability
- Light profile: ~5-10% failure rate (typical network)
- Moderate/High: 10-20% (stress testing)
- Byzantine: 10-20% (adversarial testing)

### Replay Count
- 3 replays: Quick convergence verification
- 5+ replays: Comprehensive determinism validation

## Next Steps

After harness validation completes:

1. **Integrate with CI/CD**
   - Add pytest runs to CI pipeline
   - Fail build if critical violations detected
   - Generate trend reports across releases

2. **Extended Scenarios**
   - Cross-family isolation test
   - Watermark regression prevention
   - Phantom state resilience
   - Lease expiration handling

3. **Load Testing**
   - Increase concurrent members (10+)
   - Increase task/plan volume (1000+)
   - Monitor RU consumption and latency

4. **Chaos Experiments**
   - Byzantine validator mode
   - Leader election storms
   - Network partition scenarios

## References

- **HPAL Design**: `docs/PROJECT_DIRECTIVE.md`
- **Database Design**: `docs/README.md`
- **API Specification**: `apps/api/main.py`
- **Data Model**: `modules/*/domain/`
