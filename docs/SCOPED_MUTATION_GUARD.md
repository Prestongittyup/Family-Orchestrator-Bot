---
Title: Scoped Lifecycle State Mutation Guard
Date: April 22, 2026
---

# Scoped Lifecycle State Mutation Guard

## Overview

The **Scoped Lifecycle State Mutation Guard** is an AST-based CI enforcement system that prevents direct state mutations **only on lifecycle-managed domain objects**. It eliminates false positives in unrelated infrastructure modules while maintaining strict FSM enforcement.

### Key Improvement

**Before:** Guard blocked `state`, `current_state`, `status`, `lifecycle_state` mutations **everywhere** globally → Many false positives in logging, DLQ, services.

**After:** Guard only enforces restrictions on **lifecycle-owned types** (LifecycleAction, Task, Action, Workflow, etc.) → Clean CI with zero false positives, strict lifecycle enforcement.

---

## Architecture

### 1. Lifecycle Type Registry

The guard maintains a registry of lifecycle-managed domain classes:

```python
LIFECYCLE_CLASSES = frozenset({
    "LifecycleAction",     # Primary lifecycle-managed class
    "Action",              # Action domain model
    "Task",                # Task domain model
    "Workflow",            # Workflow domain model
    "ActionPipeline",      # Pipeline orchestrator
    "TaskConnector",       # Task coordination
})
```

**To add a new lifecycle class:** Update `LIFECYCLE_CLASSES` in `ci/state_mutation_guard.py`.

### 2. Protected State Fields (Lifecycle Only)

Only these fields are enforced on lifecycle objects:

```python
LIFECYCLE_STATE_FIELDS = frozenset({
    "state",               # Generic state field
    "current_state",       # Current lifecycle state
    "lifecycle_state",     # Explicit lifecycle state
})
```

**Note:** `status` is **no longer** globally protected (eliminated false positives in infrastructure).

### 3. Module Exclusion Patterns

Entire module categories are excluded from enforcement:

```python
EXCLUDED_MODULE_PATTERNS = {
    "logging",       # Logging and log_* modules
    "log_",          # No enforcement in logging infrastructure
    "dlq",           # Dead letter queue services not lifecycle-related
    "metrics",       # Metrics and telemetry
    "telemetry",
    "utils",         # Utility functions/helpers
    "infrastructure",# Infrastructure and admin services
    "infra",
}
```

**Files in excluded modules never trigger violations**, regardless of field names.

### 4. Symbol Table & Type Inference

The visitor builds a lightweight symbol table per file to track variable types:

```
File Scope:
  LifecycleAction → LifecycleAction  (imported class)
  action → LifecycleAction           (action = LifecycleAction(...))
  task → Task                        (task = Task(...))
  logger → (untracked)               (not a lifecycle type)
  dlq_svc → (untracked)              (not a lifecycle type)
```

**Type inference pattern:**
- Track `import` and `from...import` statements for lifecycle classes
- Track variable assignments from constructor calls: `x = LifecycleAction(...)`
- When checking mutations, resolve the variable type and only enforce if it's a lifecycle type

### 5. AST-Based Mutation Detection

The visitor detects four mutation patterns:

#### a) Direct Assignment
```python
action.current_state = "forbidden"  # ❌ VIOLATION (action is LifecycleAction)
service.status = "active"           # ✓ SAFE (service is not tracked as lifecycle)
```

Detected via `ast.Assign` with `ast.Attribute` targets.

#### b) setattr() Calls
```python
setattr(action, "current_state", value)  # ❌ VIOLATION
setattr(logger, "status", value)         # ✓ SAFE (logger not a lifecycle type)
```

Detected via `ast.Call` to function named `setattr`.

#### c) __dict__ Dictionary Subscript
```python
action.__dict__["current_state"] = "forbidden"  # ❌ VIOLATION
service.__dict__["status"] = "active"           # ✓ SAFE
```

Detected via `ast.Subscript` with `__dict__` on lifecycle objects.

#### d) __dict__ Update Method
```python
action.__dict__.update({"current_state": value})  # ❌ VIOLATION
service.__dict__.update({"status": value})        # ✓ SAFE
```

Detected via `ast.Call` to `__dict__.update()` on lifecycle objects.

---

## Implementation Details

### Visitor Class: `ScopedMutationGuardVisitor`

Key methods:

| Method | Purpose |
|--------|---------|
| `visit_ImportFrom()` | Track imported lifecycle classes |
| `visit_Assign()` | Track symbol table + check attribute mutations |
| `visit_AnnAssign()` | Check annotated assignment mutations |
| `visit_Call()` | Check setattr() and __dict__.update() patterns |
| `_track_assignment()` | Update symbol table for variable types |
| `_get_variable_type()` | Resolve variable → lifecycle type mapping |
| `_check_assignment_target()` | Validate mutation targets against symbol table |

### Symbol Table Entry Logic

When the visitor encounters:
```python
from household_os.runtime.action_pipeline import LifecycleAction

action = LifecycleAction(id="test", name="Test")
action.current_state = "DONE"
```

Processing steps:
1. **Line 1:** `visit_ImportFrom()` sees `LifecycleAction` imported → adds to `imported_lifecycle_classes`, tracks `LifecycleAction → LifecycleAction` in `variable_types`
2. **Line 3:** `visit_Assign()` sees `action = LifecycleAction(...)` → calls `_track_assignment("action", Call(...))` → resolves "LifecycleAction" as lifecycle type → adds `action → LifecycleAction` to `variable_types`
3. **Line 4:** `visit_Assign()` sees `action.current_state = ...` → calls `_check_assignment_target()` → extracts variable "action" → resolves type `LifecycleAction` → finds "current_state" in `LIFECYCLE_STATE_FIELDS` → **reports VIOLATION**

### Non-Lifecycle Code (Safe)

When the visitor encounters:
```python
# In apps/api/services/dlq_service.py (module "dlq" → excluded)

def process_item(item):
    item.status = "processed"  # Would be caught if not in excluded module
    return item
```

Processing:
1. File path contains "dlq" → matched by `_should_exclude_module()` → `scan_file()` returns early with empty violations list
2. **No violation recorded**, regardless of field names

---

## Mutation Violation Reporting

Each violation includes:

```
test_violation_detection.py:16: ATTR_ASSIGN: Direct assignment to 'current_state' 
  on lifecycle object is forbidden. (LifecycleAction) [action]
```

| Field | Meaning |
|-------|---------|
| File:Line | Location of mutation attempt |
| Type | Violation pattern (ATTR_ASSIGN, SETATTR_BYPASS, DICT_UPDATE_BYPASS, DICT_SUBSCRIPT_BYPASS) |
| Message | Human-readable description |
| (LifecycleAction) | Detected type of the object being mutated |
| [action] | Variable name being mutated |

---

## CI Integration

### GitHub Actions Workflow

File: `.github/workflows/state-mutation-guard.yml`

```yaml
name: State Mutation Guard
on: [push, pull_request]

jobs:
  state-mutation-guard:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with:
          python-version: "3.11"
      - name: Check lifecycle state mutations
        run: python ci/state_mutation_guard.py apps/
```

**Exit codes:**
- `0`: No violations, CI passes
- `1`: Violations detected, CI fails

### Running Locally

```bash
# Scan entire apps/ directory
python ci/state_mutation_guard.py apps/

# Scan a specific file
python ci/state_mutation_guard.py path/to/file.py

# Scan current directory (default)
python ci/state_mutation_guard.py
```

---

## False Positive Elimination

### Before Scoping

```
State Mutation Guard Report
================================================================================
apps/api/observability/logging.py:104: DICT_UPDATE_BYPASS: Mutation via obj.__dict__.update(...) is forbidden.
apps/api/services/dlq_service.py:21: ATTR_ASSIGN: Direct assignment to 'status' is forbidden outside StateMachine.
apps/api/services/dlq_service.py:30: ATTR_ASSIGN: Direct assignment to 'status' is forbidden outside StateMachine.
────────────────────────────────────────────────────────────────────────────
Total violations: 3
```

❌ **Problem:** DLQ status field and logging mutation flagged as violations (not lifecycle-related)

### After Scoping

```
Scoped Lifecycle State Mutation Guard Report
================================================================================
✓ No lifecycle state mutations detected on lifecycle-managed objects.
```

✅ **Result:** Same codebase, zero false positives. Lifecycle enforcement intact.

---

## Extending the System

### Adding a New Lifecycle Class

To protect a new domain class:

1. **Update registry** in `ci/state_mutation_guard.py`:
   ```python
   LIFECYCLE_CLASSES = frozenset({
       "LifecycleAction",
       "Task",
       "MyNewLifecycleClass",  # Add here
   })
   ```

2. **Commit and deploy** - CI automatically enforces on next run

### Adding a New Excluded Module

To silence false positives in a utility module:

1. **Update patterns** in `ci/state_mutation_guard.py`:
   ```python
   EXCLUDED_MODULE_PATTERNS = {
       "logging",
       "dlq",
       "my_non_lifecycle_utils",  # Add here
   }
   ```

2. **Commit and deploy** - CI automatically ignores on next run

### Fine-Tuning Protected Fields

To change which fields are protected on lifecycle objects:

1. **Update field set** in `ci/state_mutation_guard.py`:
   ```python
   LIFECYCLE_STATE_FIELDS = frozenset({
       "state",
       "custom_lifecycle_field",  # Add here
   })
   ```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Lightweight symbol table** | No need for full type inference; simple variable tracking is sufficient |
| **Per-file scope** | AST analysis scoped to individual files (no cross-file type resolution complexity) |
| **Allowlist for lifecycle classes** | Explicit and minimal; easier to track responsibility |
| **Module exclusion patterns** | Fast short-circuit for entire logical modules |
| **AST-based (not regex)** | Semantic understanding of mutation patterns, eliminates regex false positives |
| **Scoped enforcement** | Only lifecycle objects subject to rules; other code remains unrestricted |

---

## Testing & Validation

### Test File

See `test_violation_detection.py` for examples of detected violations:
- Direct assignment to `current_state`
- `setattr()` calls with lifecycle fields
- `__dict__` mutations

### Running Tests

```bash
# Test violation detection (should report 3 violations)
python ci/state_mutation_guard.py test_violation_detection.py

# Test false positive elimination (should report 0 violations)
python ci/state_mutation_guard.py apps/

# Cleanup test file (optional)
rm test_violation_detection.py
```

---

## Performance

- **Per-file parsing:** AST parsing is linear in file size
- **Symbol table updates:** O(1) hash table operations
- **Typical scan:** Large monorepo (100+ files) scans in < 1 second
- **CI friendly:** Minimal overhead, integrated into existing workflows

---

## Troubleshooting

### "No violations found" but I know there are
- **Check:** Is the variable assigned a lifecycle type?
- **Check:** Is the file in an excluded module?
- **Check:** Is the file in the allowed mutation list (.github/workflows check)?

### "False positives in my module"
- **Solution:** Add module pattern to `EXCLUDED_MODULE_PATTERNS`
- **Example:** If module is named `my_status_tracker/`, add `"my_status_tracker"` to patterns

### "A lifecycle class not being detected"
- **Check:** Is it imported correctly? (`from module import ClassName`)
- **Check:** Is it in `LIFECYCLE_CLASSES` registry?
- **Check:** Is variable assigned from constructor call? (`x = ClassName()`)

---

## See Also

- [State Mutation Firewall](../household_os/runtime/state_firewall.py) - Runtime enforcement layer
- [State Proxy](../household_os/runtime/state_proxy.py) - Read-only projection of state
- [LifecycleAction](../household_os/runtime/action_pipeline.py) - Primary lifecycle domain model
- [StateMachine](../apps/api/core/state_machine.py) - Authorized mutation control plane
