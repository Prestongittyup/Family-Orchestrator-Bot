# Family Orchestration Bot

A modular household orchestration backend with built-in evaluation intelligence layer. This system generates daily household briefs with priorities, conflict detection, and task allocation—while maintaining a deterministic, pytest-based evaluation framework to measure orchestration decision quality.

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Core Components](#core-components)
4. [How to Run](#how-to-run)
5. [Evaluation System](#evaluation-system)
6. [Data Flow](#data-flow)
7. [API Endpoints](#api-endpoints)
8. [UI Dashboard](#ui-dashboard)
9. [Testing Strategy](#testing-strategy)
10. [Deployment Notes](#deployment-notes)
11. [Restoration on New Machine](#restoration-on-new-machine)
12. [Documentation Maintenance](#documentation-maintenance)

---

## Overview

### What It Does

The Family Orchestration Bot solves the **household coordination problem**: given a household with multiple members, calendars, preferences, tasks, and competing priorities, generate a daily brief that:

- Identifies the most important events and tasks
- Detects schedule conflicts early
- Allocates work fairly across household members
- Provides actionable intelligence (failures, gaps, recommendations)

### Production and Evaluation Surfaces

**PRODUCTION: Live Orchestration**
- Accepts household state and calendar data
- Generates real-time briefs via `GET /brief/{household_id}`
- Returns prioritized events, conflicts, task allocation

**PRODUCTION: Operational Product Layer**
- Exposes a strict product-facing contract for daily execution surfaces
- Generates normalized operational payloads via:
  - `GET /operational/run`
  - `GET /operational/context`
  - `GET /operational/brief`
- Uses integration pipeline outputs only (state + decision context), without coupling to evaluation/simulation routes

**EVALUATION: Deterministic Testing**
- Runs synthetic household scenarios (via pytest)
- Scores orchestration decisions across 8 dimensions
- Produces failure patterns and improvement recommendations
- Enables regression detection between runs

**ANALYSIS: Insight Feedback Bridge**
- Reads `evaluation_results.json`, `simulation_results.json`, and `operational_mode_report.json`
- Produces system-level insights, health summaries, and recommendations
- Never executes or modifies production, evaluation, simulation, or operational pipelines

**PLANNING: Policy + Memory + Recommendation Engine**
- Builds persistent household memory from emitted artifacts only
- Produces policy suggestions and daily itinerary recommendations for review
- Never writes to calendars, never executes actions, and never mutates orchestration behavior

**ASSISTANT: Orchestration Assistant Core**
- Accepts natural-language planning requests for appointments, meals, fitness, and household coordination
- Reads orchestration state, policy artifacts, evaluation artifacts, and simulation artifacts without mutating them
- Returns deterministic structured plans, reasoning traces, and inert proposed actions that require explicit approval

---

## System Architecture

### High-Level Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                      PRODUCTION PIPELINE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  FastAPI HTTP                 OrchestrationCore               │
│     │                              │                           │
│     ├─→ GET /brief/{hid} ─→ Orchestrator ─────┐              │
│     │                           │               │              │
│     │                         StateBuilder      │              │
│     │                           │               │              │
│     │                         Providers         │              │
│     │                      (calendar, etc)      │              │
│     │                           │               │              │
│     ├─→ POST /event ────→ Router Service ─→ EventHandlers    │
│                                 │               │              │
│                              SQLite      DecisionEngine        │
│     ┌──────────────────────────┘               │              │
│     │                          BriefBuilder ←──┘              │
│     │                             │                            │
│     └──→ Response: {brief, status, generated_at}             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      EVALUATION PIPELINE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Pytest Runner                 EvaluationCore                 │
│     │                              │                           │
│     ├─→ test_brief_evaluation ─→ ScenarioGenerator           │
│     │                             │                            │
│     │                          ScenarioRunner                  │
│     │                        (uses BriefBuilder)              │
│     │                             │                            │
│     │                       ScoringEngine (8 dimensions)       │
│     │                             │                            │
│     │                       FeedbackEngine                     │
│     │                        (patterns, gaps)                  │
│     │                             │                            │
│     └──→ evaluation_results.json (artifacts)                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    INSIGHT FEEDBACK BRIDGE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Artifact Readers              Insight Layer                   │
│     │                              │                           │
│     ├─→ evaluation_results.json ───┤                           │
│     ├─→ simulation_results.json ───┼─→ PatternAnalyzer         │
│     ├─→ operational_mode_report ───┤   → InsightGenerator      │
│     │                              │                           │
│     └─→ /insights/summary          │                           │
│     └─→ /insights/patterns         │                           │
│     └─→ /insights/recommendations  │                           │
│                                     └─→ insight_report.json    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│          POLICY + MEMORY + RECOMMENDATION ENGINE               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Artifact Readers              Suggestion Layer                │
│     │                              │                           │
│     ├─→ insight_report.json ───────┤                           │
│     ├─→ evaluation_results.json ───┤                           │
│     ├─→ simulation_results.json ───┼─→ memory_store           │
│     ├─→ operational_mode_report ───┤   → policy_engine        │
│     │                              │   → itinerary_generator   │
│     └─→ /policy/*                  │                           │
│                                     └─→ policy_engine_report   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│         HOUSEHOLD STATE MANAGER + DECISION ENGINE             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Natural Language             Unified State Graph             │
│     │                              │                           │
│     ├─→ /assistant/run ────────────┤                           │
│     ├─→ /assistant/query ──────────┤                           │
│     ├─→ /assistant/suggestions/* ──┼─→ household_state         │
│     ├─→ /assistant/approve ────────┤   → decision_engine       │
│     │                              │   → assistant/runtime     │
│     ├─→ integration_core (read) ───┤   → apps/assistant_core   │
│     ├─→ calendar/meal/fitness ─────┤   → state history         │
│     ├─→ policy/eval/sim artifacts ─┤   → reasoning traces      │
│     │                              │                           │
│     └─→ assistant_core_report.json │                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         UI DASHBOARD (React/Vite)              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  3 Mode System:                                                │
│                                                                 │
│  🔴 LIVE ORCHESTRATION (default)                              │
│     └─→ GET /brief, display real household state              │
│                                                                 │
│  ⚗️  SIMULATION                                                 │
│     └─→ Run evaluation, show synthetic scenario results       │
│                                                                 │
│  📊 INSIGHTS  (debug/analytics)                                │
│     └─→ GET /evaluation_results.json, display scoring,        │
│         failure patterns, decision gaps, recommendations      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                       DOCKER COMPOSITION                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  backend:8000 ←──→ ui:5173                                    │
│  (FastAPI)          (React/Vite)                              │
│                                                                 │
│  Docker Bridge Network: http://backend:8000                   │
│  UI proxies API calls via VITE_BACKEND_URL env var           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### Backend: Integration Core (`integration_core/`)

**Orchestrator** (`orchestrator.py`)
- Entry point for household brief generation
- Coordinates state building, decision processing, brief construction
- Accepts optional DecisionEngine for custom prioritization
- No business logic—purely orchestration

**State Builder** (`state_builder.py`)
- Aggregates household state from multiple providers
- Collects calendar events, tasks, preferences, constraints
- Returns structured HouseholdState object

**Brief Builder** (`brief_builder.py`)
- Consumes HouseholdState + DecisionContext
- Generates final brief with:
  - `summary`: text summary
  - `top_events`: prioritized calendar events
  - `today_events`: all events for today
  - `conflicts`: detected schedule overlaps
  - `task_allocation`: tasks grouped by member
  - `next_upcoming_event`: immediate next action

**Decision Engine** (`decision_engine.py`)
- Applies prioritization logic to state
- Produces DecisionContext containing:
  - `priority_ranking`: sorted event/task sequence
  - `conflict_alerts`: schedule conflicts
  - `allocation`: member task assignments
- Can be injected or instantiated with defaults

**Providers** (`providers.py`)
- Abstract interfaces for data sources
- Calendar provider (Google Calendar, local mock)
- Task provider (database, mock)
- Member preference provider

### Read-Only Insight Layer (`insights/`)

**Insight Engine** (`insight_engine.py`)
- Loads and validates emitted JSON artifacts
- Builds strict insight responses with no pipeline execution
- Generates `insight_report.json`

**Pattern Analyzer** (`pattern_analyzer.py`)
- Detects cross-artifact patterns including:
  - `priority_misalignment`
  - `conflict_resolution_failure`
  - `decision_instability`
  - `omission_bias`
  - `system_drift`

**Insight Generator** (`insight_generator.py`)
- Produces structured, non-invasive insight statements
- Produces recommendation objects without mutating system behavior

**Insight Router** (`insight_router.py`)
- Exposes read-only endpoints for summary, patterns, and recommendations
- Has no runtime dependency on orchestration, evaluation, simulation, or operational execution code paths

### Policy Recommendation Layer (`policy_engine/`)

**Memory Store** (`memory_store.py`)
- Builds deterministic household memory from artifacts
- Persists policy-owned memory snapshots without mutating execution layers
- Owns policy-layer file persistence boundaries

**Policy Engine** (`policy_engine.py`)
- Converts memory + artifacts into suggestion-only policy recommendations
- Produces priority, scheduling, conflict, and routine optimization suggestions
- Generates policy-layer report summaries without changing system behavior

**Itinerary Generator** (`itinerary_generator.py`)
- Produces daily itinerary recommendations from memory and current date context
- Prioritizes medical, school, work, and household coordination signals
- Surfaces conflicts for manual review instead of execution

**Policy Router** (`policy_router.py`)
- Exposes memory, policy summary, itinerary, and recompute endpoints
- Keeps recompute user-triggered and suggestion-only

### Assistant Core (`apps/assistant_core/`)

**Intent Parser** (`intent_parser.py`)
- Converts natural-language requests into deterministic intent objects
- Supports appointment, meal, fitness, and general coordination request types
- Extracts entities, time constraints, priority hints, and context flags

**Planning Engine** (`planning_engine.py`)
- Produces candidate schedules, conflict analysis, recommended plans, and fallback options
- Uses read-only orchestration state when available and deterministic fallback state otherwise
- Reads evaluation, simulation, insight, and policy artifacts strictly for context and reasoning traces

**Meal Planner** (`meal_planner.py`)
- Ranks meal candidates from inventory plus recipe history
- Prevents recipe repeats within a configurable 7-14 day window
- Emits grocery additions only when inventory coverage is incomplete

**Fitness Planner** (`fitness_planner.py`)
- Builds schedule-aware weekly workout plans from available windows
- Generates insertion suggestions only, never live calendar writes

**Assistant Router** (`assistant_router.py`)
- Exposes query, retrieval, and approval endpoints
- Persists assistant responses only inside an isolated in-memory request store
- Logs reasoning traces for every request and keeps approvals inert

### Assistant Runtime (`assistant/`)

**Runtime Engine** (`runtime/assistant_runtime.py`)
- Serves as the unified assistant entry point for calendar, meal, fitness, and household coordination decisions
- Builds one deterministic runtime plan from raw input plus a read-only household state snapshot
- Merges domain proposals and resolves cross-domain conflicts without mutating household state

**State Snapshot Service** (`state/state_snapshot.py`)
- Aggregates read-only calendar events, recent meals, fitness schedule windows, and household context
- Produces isolated snapshot data used by the runtime and Today Dashboard
- Performs no writes and deep-copies source state before projection

**Plan Merger** (`planning/plan_merger.py`)
- Ranks proposals globally across domains
- Detects cross-domain overlap conflicts from unified time blocks
- Produces one ordered `AssistantPlan` contract for runtime consumers

**Unified Runtime Contract** (`contracts/assistant_plan.py`)
- Defines `AssistantPlan` with:
  - `request_id`
  - `intent`
  - `state_snapshot`
  - `proposals`
  - `conflicts`
  - `ranked_plan`
  - `requires_approval`
  - `execution_payload`

### Daily Loop Engine (`assistant/daily_loop/`)

**Daily Loop Engine** (`daily_loop_engine.py`)
- Wraps `AssistantRuntimeEngine` or consumes an existing runtime plan directly
- Produces one full-day `DailyPlan` with segmented schedule blocks, meal/workout placement, gaps, conflicts, and approval state
- Keeps preview generation read-only and leaves persistence to the explicit regenerate endpoint only

**Time Slicer** (`time_slicer.py`)
- Defines fixed morning, midday, and evening day segments
- Allocates proposal blocks while preserving buffer time and preventing overlaps
- Detects open scheduling gaps after calendar constraints and assistant placements are applied

**Day Builder** (`day_builder.py`)
- Merges runtime proposals with calendar constraints from the runtime snapshot
- Resolves placements deterministically using segment order, start time, lock state, and item identity
- Emits a conflict-free `DailyPlan` whenever safe placement is possible and surfaces structured conflicts when it is not

### Backend: FastAPI Layer (`apps/api/`)

**Brief Endpoint** (`endpoints/brief_endpoint.py`)
```
GET /brief/{household_id}
  ?user_id=optional
  &include_trace=false
  &include_observability=false
  &render_human=false
  &validate_contract_v1=false
```
Returns: `{status, brief, generated_at, [debug], [rendered]}`

**Event Ingestion Endpoint** (`endpoints/`)
```
POST /event
  {type, payload, ...}
```
Routes events to module handlers, persists to SQLite, returns created entity.

**Evaluation Router** (`endpoints/evaluation_router.py`)
- `GET /evaluation_results.json`: Serves latest evaluation artifact
- `GET /evaluation/run`: Triggers pytest evaluation, returns execution summary

**Integrations Router** (`endpoints/integrations_router.py`)
- OAuth credential management
- Third-party service callbacks
- UI routing

**Assistant Router** (`../assistant_core/assistant_router.py`)
- `POST /assistant/run`: produce a user-facing `HouseholdDecisionResponse` from the household state graph and decision engine
- `GET /assistant/daily`: preview a read-only full-day `DailyPlan`
- `POST /assistant/daily/regenerate`: explicitly regenerate and persist approval state for a full-day `DailyPlan`
- `POST /assistant/query`: parse a natural-language request into one deterministic household decision
- `GET /assistant/suggestions/{request_id}`: retrieve a stored deterministic household decision
- `POST /assistant/approve`: record approval for the single recommended next action without downstream execution

### Evaluation System (`tests/evaluation/`)

**Scenario Models** (`scenario_models.py`)
```python
@dataclass
class HouseholdScenario:
    scenario_id: str
    description: str
    household_members: list[str]
    events: list[ScenarioEvent]
    expected_signals: dict
    expected_outcomes: dict
```

**Scenario Generator** (`scenario_generator.py`)
- Creates 5+ deterministic household scenarios
- Includes:
  - Busy day with conflicts
  - Priority alignment challenges
  - Multi-member coordination
  - Low-signal noise filtering
  - Holiday/special event handling

**Scenario Runner** (`brief_runner.py`)
- Executes each scenario against BriefBuilder
- Builds synthetic HouseholdState
- Captures brief output

**Scoring Engine** (`scoring_engine.py`)
- Evaluates brief against expected outcomes
- 8 scoring dimensions (0-10 scale):
  1. **priority_score**: Top events ranked correctly
  2. **relevance_score**: Unnecessary items excluded
  3. **completeness_score**: Critical events included
  4. **clarity_score**: Summary is actionable
  5. **priority_correctness**: Ranking matches human expectation
  6. **conflict_handling_score**: Detected and highlighted conflicts
  7. **omission_score**: Critical items not missed
  8. **noise_penalty**: Low-value items not inflated

**Feedback Engine** (`feedback_engine.py`)
- Extracts failure patterns from scoring (e.g., "priority_misordering")
- Maps patterns to decision gaps (e.g., "priority_weighting_logic_weak")
- Generates recommendations with priority levels

**Evaluation Runner** (`evaluation_runner.py`)
- Orchestrates full pipeline:
  1. ScenarioGenerator → scenarios
  2. For each scenario: ScenarioRunner → brief
  3. Per-brief: ScoringEngine → scores
  4. Aggregate scores across all scenarios
  5. Compare against previous run (deltas, regressions)
  6. Extract failure patterns, gaps, recommendations
  7. Write evaluation_results.json artifact
  8. Print "DECISION_FEEDBACK_COMPLETE"

### UI Dashboard (`ui/src/`)

**App.jsx** - Mode router
- Manages mode state: "LIVE" | "SIMULATION" | "INSIGHTS"
- Renders TopNav + ModeSelector + mode-specific component

**Components: Core**
- `TopNav.jsx`: "🏠 Household Command Center" header
- `ModeSelector.jsx`: 3-mode tab navigation

**Components: Live Mode** (`components/live/`)
- `OrchestrationDashboard.jsx`: Main container, fetches `/brief/{household_id}`
- `HouseholdBriefPanel.jsx`: Displays brief summary
- `PriorityTimeline.jsx`: Timeline visualization of priorities
- `ConflictMonitor.jsx`: Alerts for schedule conflicts
- `MemberTaskView.jsx`: Grid of members with allocated tasks

**Components: Simulation Mode** (`components/simulation/`)
- `ScenarioRunner.jsx`: Wraps EvaluationRunner, shows execution + summary

**Components: Insights Mode** (`components/insights/`)
- `EvaluationDashboard.jsx`: Full evaluation visualizations
  - `ScenarioTable.jsx`: Browse all scenarios + scores
  - `ScenarioDetail.jsx`: Deep-dive into selected scenario
  - `MetricsPanel.jsx`: Aggregate 8-dimension metrics
  - `DecisionPanel.jsx`: Failure patterns, gaps, recommendations
  - `ComparisonPanel.jsx`: Run-to-run deltas and regressions
  - `TodayDailyView.jsx`: Full-day assistant timeline with meals, workouts, conflicts, gaps, and full-day approval

**Styling**: `index.css`
- Dark theme (GitHub-inspired)
- Responsive grid layouts
- Component-scoped styles

### Docker Setup

**Dockerfile.backend**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**ui/Dockerfile.ui**
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json .
RUN npm ci
COPY . .
RUN npm run build
EXPOSE 5173
CMD ["npm", "run", "preview", "--", "--host", "0.0.0.0", "--port", "5173"]
```

**docker-compose.yml**
```yaml
services:
  backend:
    build: . (Dockerfile.backend)
    ports: [8000:8000]
    volumes: [.:/app]
    env: PYTHONUNBUFFERED=1

  ui:
    build: ./ui (Dockerfile.ui)
    ports: [5173:5173]
    depends_on: [backend]
    env:
      VITE_BACKEND_URL=http://backend:8000
      VITE_API_BASE_URL=/api
```

---

## How to Run

### Option A: Docker (Recommended)

**Prerequisites**
- Docker + Docker Compose
- (No Python, Node.js, or npm required)

**Start System**
```bash
cd /path/to/Family\ Orchestration\ Bot
docker-compose up --build
```

**Expected Output**
```
backend    | Uvicorn running on http://0.0.0.0:8000
ui         | VITE v5.x.x  ready in x ms
ui         | ➜  Local: http://localhost:5173/
```

**Access**
- UI: http://localhost:5173
- Backend API: http://localhost:8000
- Brief endpoint: http://localhost:8000/docs

**Run Evaluation via UI**
1. Switch to "SIMULATION" mode (⚗️)
2. Click "Run Evaluation"
3. View results in "INSIGHTS" mode (📊)

**Teardown**
```bash
docker-compose down
```

### Option B: Local Development

**Prerequisites**
- Python 3.11+
- Node.js 18+
- npm

**Backend Setup**
```bash
cd /path/to/Family\ Orchestration\ Bot
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate.ps1
pip install -r requirements.txt
```

**Start Backend**
```bash
uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload
```

**UI Setup** (in new terminal)
```bash
cd ui
npm install
npm run dev
```

**Access**
- UI: http://localhost:5173
- Backend: http://localhost:8000
- Vite proxy routes /api calls to http://localhost:8000

**Run Evaluation Locally**
```bash
pytest tests/test_brief_evaluation.py -s
```

Produces: `evaluation_results.json` in project root

### Troubleshooting

**"ModuleNotFoundError: No module named 'apps'"**
- Ensure you're running from project root
- Check PYTHONPATH includes project root
- Use full module path: `python -m pytest ...`

**"Cannot find node_modules"**
- Docker: Automatically resolved in build
- Local: Run `npm ci` in `ui/` directory

**"Port 8000 already in use"**
- Docker: `docker-compose down`, then retry
- Local: `netstat -ano | find ":8000"` (Windows) or `lsof -i :8000` (Mac/Linux)

**VITE Build Failure**
- Clear cache: `rm -rf ui/dist ui/node_modules/.vite`
- Reinstall: `cd ui && npm ci && npm run build`

---

## Evaluation System

### Running Evaluations

**Via Docker (Recommended)**
```bash
docker-compose run backend pytest tests/test_brief_evaluation.py -s
```

**Via Local Python**
```bash
pytest tests/test_brief_evaluation.py -s
```

**Via UI (Browser)**
1. Go http://localhost:5173
2. Switch to "SIMULATION" mode
3. Click "Run Evaluation"
4. Switch to "INSIGHTS" mode to view results

### Artifact: evaluation_results.json

Structure:
```json
{
  "scenarios": [
    {
      "scenario_id": "s001_busy_day",
      "description": "...",
      "scores": {
        "priority_score": 8,
        "relevance_score": 7,
        "completeness_score": 9,
        "clarity_score": 8,
        "priority_correctness": 8,
        "conflict_handling_score": 9,
        "omission_score": 9,
        "noise_penalty": 7
      },
      "issues": ["low_priority_item_included", "...]"
    }
  ],
  "aggregate": {
    "avg_priority": 7.8,
    "avg_relevance": 7.4,
    "avg_completeness": 8.6,
    "avg_clarity": 8.2,
    "avg_priority_correctness": 7.9,
    "avg_conflict_handling": 8.8,
    "avg_omission": 8.5,
    "avg_noise_penalty": 7.1
  },
  "comparison": {
    "improved": false,
    "regressions": ["priority_score", "..."],
    "score_deltas": {
      "priority_score": -0.5,
      "relevance_score": 0.2,
      ...
    }
  },
  "failure_patterns": [
    {
      "type": "priority_misordering",
      "count": 2,
      "scenarios": ["s001", "s003"]
    }
  ],
  "decision_gaps": [
    {
      "gap_type": "priority_weighting_logic_weak",
      "source_failure": "priority_misordering",
      "frequency": 2
    }
  ],
  "recommended_adjustments": [
    {
      "gap": "priority_weighting_logic_weak",
      "recommendation": "Increase weighting of time-sensitive events...",
      "priority": "high"
    }
  ]
}
```

### Scoring Dimensions (0-10 scale)

| Dimension | Definition |
|-----------|-----------|
| **priority_score** | Top 3+ events ranked in correct order |
| **relevance_score** | No low-signal items in brief |
| **completeness_score** | All critical events included |
| **clarity_score** | Summary is actionable + unambiguous |
| **priority_correctness** | Overall ranking matches human assessment |
| **conflict_handling_score** | Schedule conflicts detected + highlighted |
| **omission_score** | No critical items omitted |
| **noise_penalty** | Non-actionable items not inflated |

### Failure Patterns

Extracted from scoring issues:
- `priority_misordering`: Events ordered incorrectly
- `conflict_handling_failure`: Conflicts not detected/highlighted
- `omission_failure`: Critical items missing
- `noise_inclusion`: Low-value items included
- `other`: Unclassified

### Decision Gaps

Mapped from failure patterns:
- `priority_weighting_logic_weak`: → "Increase event time-sensitivity weighting"
- `conflict_detection_missing_or_insufficient`: → "Add explicit schedule conflict detection"
- `event_selection_filtering_too_aggressive`: → "Relax filtering thresholds"
- `relevance_filtering_too_permissive`: → "Tighten relevance criteria"
- `unclassified_decision_gap`: → "Manual review required"

### Regression Detection

Each evaluation run compares against `evaluation_results.json`:
- If any metric decreased: `regressions` list populated
- Deltas show: current_value - previous_value
- `improved` flag: true only if no regressions AND some positive deltas

---

## Data Flow

### 1. Event Ingestion (Production Runtime)

```
POST /event  {"type": "calendar_event", "payload": {...}}
  ↓
Router → route_event(SystemEvent)
  ↓
Module Handler (calendar, task, etc.)
  ↓
Persist to SQLite
  ↓
Event Bus → notify listeners
  ↓
Response: {status, result}
```

### 2. Brief Generation (GET /brief/{household_id})

```
GET /brief/{household_id}
  ↓
Orchestrator.build_household_state(user_id)
  ↓
StateBuilder aggregates:
  - Calendar events (from Google Calendar or mock)
  - Tasks (from SQLite)
  - Preferences (from config/database)
  - Member info
  ↓
Returns: HouseholdState {members, events, tasks, constraints, ...}
  ↓
DecisionEngine.process(state)
  ↓
Returns: DecisionContext {priority_ranking, conflicts, allocation}
  ↓
BriefBuilder.build(state, decision_context)
  ↓
Returns: Brief {summary, top_events, today_events, conflicts, ...}
  ↓
Response: {status: "success", brief, generated_at}
```

### 3. Evaluation Pipeline

```
pytest tests/test_brief_evaluation.py -s
  ↓
ScenarioGenerator.generate_scenarios() → [Scenario, Scenario, ...]
  ↓
For each scenario:
  ╔════════════════════════════════════════╗
  ║ ScenarioRunner.run_scenario(scenario)  ║
  ║   ↓                                    ║
  ║   Build synthetic HouseholdState       ║
  ║   (mock calendar, tasks, members)      ║
  ║   ↓                                    ║
  ║   Orchestrator.build_household_state() ║
  ║   (uses BriefBuilder unchanged)        ║
  ║   ↓                                    ║
  ║   BriefBuilder.build() → brief         ║
  ║   ↓                                    ║
  ║   Return brief                         ║
  ╚════════════════════════════════════════╝
  ↓
ScoringEngine.evaluate_brief(brief, expected)
  ↓
Returns: {priority_score, relevance_score, ..., issues: [...]}
  ↓
Aggregate across all scenarios
  ↓
Load previous evaluation_results.json (if exists)
  ↓
Compare: score deltas, regressions
  ↓
FeedbackEngine.extract_failure_patterns(results)
  ↓
FeedbackEngine.map_failure_to_decision_gaps(patterns)
  ↓
FeedbackEngine.generate_recommendations(gaps)
  ↓
Write: evaluation_results.json
  ↓
Print: "DECISION_FEEDBACK_COMPLETE"
```

---

## API Endpoints

### GET /brief/{household_id}

**Description**: Generate household brief for given household and user.

**Query Parameters**
| Param | Type | Default | Purpose |
|-------|------|---------|---------|
| `user_id` | string | `{household_id}` | User filtering (optional) |
| `include_trace` | bool | false | Include decision trace |
| `include_observability` | bool | false | Include metrics snapshot |
| `render_human` | bool | false | Include human-readable rendered brief |
| `validate_contract_v1` | bool | false | Validate against BriefContractV1 |

**Request**
```
GET /brief/household_001
GET /brief/household_001?user_id=user_123&render_human=true
```

**Response (200 OK)**
```json
{
  "status": "success",
  "brief": {
    "summary": "4 events today. 1 conflict detected. Alice: 3 tasks, Bob: 2 tasks.",
    "top_events": [
      {
        "title": "Team standup",
        "start_time": "2025-04-18T09:00:00Z",
        "end_time": "2025-04-18T09:30:00Z",
        "priority": "high"
      }
    ],
    "today_events": [...],
    "conflicts": [
      {
        "participants": ["Alice", "Bob"],
        "event1": "Meeting with Bob",
        "event2": "Dentist appointment",
        "overlap_start": "2025-04-18T14:00:00Z",
        "overlap_end": "2025-04-18T14:30:00Z"
      }
    ],
    "task_allocation": {
      "Alice": ["Grocery shopping", "Laundry"],
      "Bob": ["Fix kitchen sink"]
    },
    "next_upcoming_event": {
      "title": "Team standup",
      "start_time": "2025-04-18T09:00:00Z"
    },
    "calendar": {
      "upcoming": [...]
    }
  },
  "generated_at": "2025-04-18T08:45:00Z",
  "debug": {
    "cache_state": "orchestrated",
    "feature_flags": {...}
  }
}
```

**Error Responses**
```json
// 400: Invalid household_id
{"detail": "Invalid household_id format"}

// 500: Orchestration failed
{"detail": "Error building household state: ..."}
```

### POST /event

**Description**: Ingest a system event (calendar, task, etc.).

**Request Body**
```json
{
  "type": "calendar_event",
  "payload": {
    "title": "Team standup",
    "start_time": "2025-04-18T09:00:00Z",
    "end_time": "2025-04-18T09:30:00Z",
    "calendar_id": "alice@example.com"
  }
}
```

**Response (200 OK)**
```json
{
  "status": "processed",
  "result": {
    "id": "evt_123",
    "type": "calendar_event",
    "processed_at": "2025-04-18T08:45:00Z"
  }
}
```

### GET /evaluation_results.json

**Description**: Retrieve latest evaluation artifact.

**Request**
```
GET /evaluation_results.json
```

**Response (200 OK)**
Returns the full `evaluation_results.json` file as JSON.

**Error Response**
```json
// 404: No evaluation run yet
{"detail": "evaluation_results.json not found"}
```

### GET /evaluation/run

**Description**: Trigger evaluation pipeline execution.

**Request**
```
GET /evaluation/run
```

**Response (200 OK)**
```json
{
  "status": "success",
  "summary": "...pytest output...",
  "artifact_path": "evaluation_results.json"
}
```

Response will contain full pytest output including "DECISION_FEEDBACK_COMPLETE" marker.

### GET /operational/run

**Description**: Execute the operational product-layer pipeline for a household and return a strict response contract.

**Request**
```
GET /operational/run?household_id=household-001
```

### GET /operational/context

**Description**: Return operational context projection from integration outputs with the same strict schema.

**Request**
```
GET /operational/context?household_id=household-001
```

### GET /operational/brief

**Description**: Return brief-focused operational projection from integration outputs with the same strict schema.

**Request**
```
GET /operational/brief?household_id=household-001
```

**Operational Response Contract**
```json
{
  "timestamp": "2026-04-18T00:00:00Z",
  "household_id": "household-001",
  "top_priorities": [
    {"title": "...", "priority_level": "high", "reason": "..."}
  ],
  "schedule_actions": [
    {"action": "...", "time": "...", "confidence": 0.9}
  ],
  "conflicts": [
    {"conflict_type": "schedule_overlap", "severity": "high", "description": "..."}
  ],
  "system_notes": ["..."]
}
```

### GET /insights/summary

**Description**: Return aggregated read-only insight bridge output from existing artifacts.

**Request**
```
GET /insights/summary
```

### GET /insights/patterns

**Description**: Return insight bridge output focused on detected pattern surfaces.

**Request**
```
GET /insights/patterns
```

### GET /insights/recommendations

**Description**: Return insight bridge output focused on generated recommendations.

**Request**
```
GET /insights/recommendations
```

**Insight Response Contract**
```json
{
  "timestamp": "2026-04-18T00:00:00Z",
  "insights": [
    {
      "type": "system_drift",
      "severity": "high",
      "description": "...",
      "evidence_sources": ["evaluation", "simulation", "operational"],
      "affected_components": ["cross_layer_alignment"]
    }
  ],
  "system_health_summary": {
    "stability_score": 1.0,
    "conflict_rate": 0.0,
    "priority_accuracy_estimate": 0.6
  },
  "recommendations": [
    {
      "recommendation": "...",
      "reason": "...",
      "priority": "medium"
    }
  ]
}
```

### GET /policy/summary

**Description**: Return high-level policy suggestions derived from artifacts and policy memory.

**Request**
```
GET /policy/summary
```

**Policy Summary Contract**
```json
{
  "policies": [
    {
      "policy_type": "priority_adjustment_suggestion",
      "description": "...",
      "reasoning": "...",
      "confidence": 0.72,
      "impact_area": ["operational", "evaluation"]
    }
  ]
}
```

### GET /policy/memory

**Description**: Return the current structured household memory snapshot.

**Request**
```
GET /policy/memory
```

### GET /policy/itinerary

**Description**: Return the current daily itinerary recommendation.

**Request**
```
GET /policy/itinerary
```

### POST /policy/recompute

**Description**: User-triggered recompute for policy memory, policy suggestions, itinerary, and policy report persistence.

**Request**
```
POST /policy/recompute
```

**Itinerary Contract**
```json
{
  "date": "2026-04-19",
  "recommended_itinerary": [
    {
      "time_block": "08:00-08:30",
      "event": "Medical readiness check",
      "reason": "...",
      "priority": "high"
    }
  ],
  "conflicts_detected": ["..."],
  "optimization_notes": ["..."]
}
```

### POST /assistant/query

**Description**: Parse a natural-language planning request and return a deterministic assistant plan.

**Request**
```
POST /assistant/query
```

```json
{
  "query": "Schedule a doctor appointment for Monday morning after school drop-off",
  "household_id": "household-001",
  "repeat_window_days": 10,
  "fitness_goal": null
}
```

### POST /assistant/run

**Description**: Run the unified assistant runtime and return one merged `AssistantPlan` across calendar, meals, fitness, and household coordination.

**Request**
```
POST /assistant/run
```

```json
{
  "query": "Plan today around school pickup with dinner and a workout block",
  "household_id": "household-001",
  "repeat_window_days": 10,
  "fitness_goal": "fat loss"
}
```

### GET /assistant/suggestions/{request_id}

**Description**: Retrieve the stored assistant response for a prior request.

**Request**
```
GET /assistant/suggestions/assist-ffafbb2c8da2
```

### POST /assistant/approve

**Description**: Record explicit approval for one or more inert proposed actions.

**Request**
```
POST /assistant/approve
```

```json
{
  "request_id": "assist-ffafbb2c8da2",
  "action_ids": ["assist-74cd66cc3565-hold"]
}
```

**Assistant Response Contract**
```json
{
  "request_id": "assist-ffafbb2c8da2",
  "intent": {
    "intent_type": "appointment",
    "entities": ["doctor", "school"],
    "time_constraints": ["monday", "morning"],
    "priority": "low",
    "context_flags": ["family_schedule"]
  },
  "plan": {
    "domain": "appointment",
    "summary": "Recommended appointment window: 2026-04-20 10:30-11:15.",
    "candidate_schedules": [],
    "recommended_plan": {
      "summary": "Book a tentative appointment hold for 2026-04-20 10:30-11:15.",
      "timeline_blocks": [],
      "confidence": 0.84,
      "reasoning": "The recommended option avoids current overlaps while keeping the request close to the preferred day and time."
    },
    "fallback_options": [],
    "meal_plan": null,
    "fitness_plan": null
  },
  "conflicts": [],
  "alternatives": [],
  "proposed_actions": [
    {
      "action_id": "assist-74cd66cc3565-hold",
      "action_type": "calendar_hold",
      "description": "Create a tentative appointment hold after human confirmation.",
      "target": "2026-04-20T10:30:00Z",
      "approval_status": "pending",
      "execution_mode": "inert_until_approved"
    }
  ],
  "reasoning_trace": ["..."]
}
```

### GET /docs

**FastAPI Interactive Docs**
```
http://localhost:8000/docs
```

Swagger UI with all endpoints, schemas, and interactive testing.

---

## UI Dashboard

### 3-Mode Architecture

**🔴 LIVE ORCHESTRATION (Default)**

Purpose: Real-time household state monitoring

Components:
- **HouseholdBriefPanel**: Summary + generated timestamp
- **PriorityTimeline**: Visual timeline of priorities
- **ConflictMonitor**: Highlighted schedule conflicts with involved members
- **MemberTaskView**: Grid of household members with task allocation

Data Source: `GET /brief/{household_id}` (auto-refreshes on button click)

UX Pattern: Command center style, no scores, operational focus

**⚗️ SIMULATION**

Purpose: Test orchestration logic with synthetic scenarios

Components:
- **ScenarioRunner**: Evaluation trigger + execution status
- **Summary Display**: Count of scenarios executed + latest timestamp

Data Flow: Click → POST /evaluation/run → pytest executes → results loaded → summary shown

UX Pattern: Test harness, clearly labeled "simulation"

**📊 INSIGHTS (Debug/Analytics)**

Purpose: Analyze orchestration performance

Components:
- **ScenarioTable**: Browse all evaluation scenarios, filter by score
- **ScenarioDetail**: Drill into single scenario, view scores + issues
- **MetricsPanel**: Aggregate 8-dimension metrics (bar charts)
- **DecisionPanel**: Failure patterns, decision gaps, recommendations
- **ComparisonPanel**: Run-to-run deltas, regressions highlighted

Data Source: `GET /evaluation_results.json`

UX Pattern: Analytics dashboard, evaluation-focused, technical audience

Inside INSIGHTS, the inspector now exposes a single user-facing assistant surface:
- **Today State View**: one state summary, one recommended next action, one approval path, all backed by `/assistant/run` and `/assistant/approve`.
- Daily-loop, policy, operational, and insights layers remain in the repo and backend, but are no longer exposed as visible module tabs in the main assistant inspector.

### Component Hierarchy

```
<App>
  <TopNav /> ("🏠 Household Command Center")
  <ModeSelector /> (LIVE | SIMULATION | INSIGHTS tabs)
  
  {mode === "LIVE" && <OrchestrationDashboard />}
    ├─ <HouseholdBriefPanel />
    ├─ <PriorityTimeline />
    ├─ <ConflictMonitor />
    └─ <MemberTaskView />

  {mode === "SIMULATION" && <ScenarioRunner />}
    └─ <EvaluationRunner />

  {mode === "INSIGHTS" && <EvaluationDashboard />}
    ├─ <ScenarioTable />
    ├─ <ScenarioDetail />
    ├─ <MetricsPanel />
    ├─ <DecisionPanel />
    └─ <ComparisonPanel />
</App>
```

### Styling

- **Theme**: Dark mode (GitHub-inspired)
- **Layout**: Responsive CSS Grid
- **colors**:
  - Primary: `--blue` (#3b82f6)
  - Success: `--green` (#22c55e)
  - Alert: `--red` (#ef4444)
  - Warning: `--yellow` (#eab308)
- **Files**: `ui/src/index.css` (500+ lines)

---

## Testing Strategy

### Test Coverage Philosophy

**Deterministic Evaluation > Unit Mocking**

Rationale: Complex orchestration logic requires end-to-end scenario testing. Synthetic household scenarios with expected outcomes reveal integration bugs that unit tests miss.

**Coverage Areas**

| Area | Strategy | File |
|------|----------|------|
| Orchestration | E2E evaluation scenarios | `test_brief_evaluation.py` |
| Decision Engine | Scoring engine assertions | `test_decision_engine.py`, `test_decision_engine_integration.py` |
| Integration | Architecture guards | `test_integration_core.py`, `test_integration_architecture_guard.py` |
| API Contracts | Endpoint response validation | `test_brief_builder.py`, `test_evaluation_endpoints.py` |
| Operational Product Layer | Contract and isolation validation | `test_operational_mode.py` |
| Insight Feedback Bridge | Artifact parsing, isolation, and pattern detection | `test_insight_bridge.py` |
| Policy + Memory Engine | Determinism, itinerary integrity, and non-execution validation | `test_policy_engine.py` |
| Assistant Core | Intent parsing, conflict detection, approval gating, and determinism validation | `test_assistant_core.py` |
| Assistant Runtime | Unified plan merging, read-only snapshots, cross-domain conflict detection, and inert execution validation | `test_assistant_runtime.py` |
| Event System | Isolated handler tests | `test_event_adapter.py`, `test_adapter_governance.py` |
| Identity | OAuth + Multi-user | `test_identity_system.py`, `test_oauth_credential_store.py` |
| Stability | System-wide regression | `test_system_stability_lock.py` |

### Test Isolation

**conftest.py** implements:
```python
# Pytest fixtures for clean test isolation:

@pytest.fixture(scope="function", autouse=True)
def clean_database(ensure_test_schema):
    """Wipe all tables before/after each test"""
    # Pre-test: DELETE FROM tasks, events, etc.
    # Post-test: Clean up
    # Ensures zero cross-test state pollution

@pytest.fixture(scope="function", autouse=True)
def reset_event_bus():
    """Reset global event registry per test"""
    # Prevents event handlers from accumulating

@pytest.fixture(scope="function", autouse=True)
def reset_runtime_feature_flags():
    """Feature flags start fresh per test"""
```

### Running Tests

**All Tests**
```bash
pytest -q
```

**Evaluation Only**
```bash
pytest tests/test_brief_evaluation.py -s
```

**Single Scenario Family**
```bash
pytest tests/evaluation/ -v
```

**With Coverage**
```bash
pytest --cov=apps --cov=integration_core --cov-report=html
```

### Expected Results

**Full Suite: 288+ tests**
- All tests pass ✓
- No cross-test contamination
- Deterministic (same results every run)
- Evaluation artifact stable (before/after identical)

**Evaluation Test: test_brief_evaluation.py**
- 5+ scenarios executed
- 8 metrics scored per scenario
- Failure patterns extracted
- Decision gaps identified
- Recommendations generated
- Output: `evaluation_results.json`

---

## Deployment Notes

### Docker Requirements

**Supported Hosts**
- Docker Desktop (Windows, macOS)
- Docker Engine (Linux)
- Docker Compose 2.0+

**Resource Requirements**
- Memory: 2GB minimum
- Disk: 1GB for images + volumes
- CPU: 2 cores minimum

**Known Issues**

1. **Node/npm in Vite Build**
   - Dockerfile.ui specifies `node:18-alpine` explicitly
   - No system npm dependency—fully containerized
   - If build fails: `docker-compose down && docker-compose up --build`

2. **Port Conflicts**
   - Backend: 8000 (adjust in docker-compose.yml if needed)
   - UI: 5173 (adjust if needed)
   - Check: `netstat -ano | findstr ":8000"` (Windows)

3. **Volume Mounts**
   - Backend mounts project root for live code reload
   - UI builds from Dockerfile, no live reload in Docker
   - For UI changes: `docker-compose up --build ui`

4. **Database Persistence**
   - SQLite at `data/family_orchestration.db`
   - Mounts to container—persists across restarts
   - To reset: `rm data/family_orchestration.db`

### Windows-Specific Setup

**PowerShell Activation** (if not using Docker)
```powershell
# Set execution policy (one-time)
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned

# Activate venv
& ".venv\Scripts\Activate.ps1"
```

**Docker Desktop for Windows**
- Must be running before `docker-compose up`
- WSL 2 backend recommended
- Ensure Docker daemon is started

### Configuration

**Environment Variables** (in docker-compose.yml)

| Var | Value | Purpose |
|-----|-------|---------|
| `PYTHONUNBUFFERED` | 1 | Real-time log output |
| `VITE_BACKEND_URL` | http://backend:8000 | UI→backend communication (Docker network) |
| `VITE_API_BASE_URL` | /api | UI API prefix |

**Google Calendar (Optional Credentials)**

In `.env`:
```
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
GOOGLE_REDIRECT_URI=http://127.0.0.1:8000/integrations/google-calendar/callback
```

Calendar integration currently uses mock providers. Real OAuth requires valid credentials + network access.

---

## Restoration on New Machine

### Step 1: Prerequisites Check

```bash
# Verify Docker + Compose
docker --version          # Must be 20.10+
docker-compose --version  # Must be 2.0+

# If missing:
# - Windows/macOS: Install Docker Desktop
# - Linux: Install Docker + Docker Compose plugin
```

### Step 2: Clone Repository

```bash
git clone <repo-url>
cd "Family Orchestration Bot"
```

### Step 3: Start System

```bash
docker-compose up --build
```

Wait for output:
```
backend    | Uvicorn running on http://0.0.0.0:8000
ui         | ➜  Local: http://localhost:5173/
```

### Step 4: Verify Endpoints

**Backend Health**
```bash
curl http://localhost:8000/docs
# Returns: Swagger UI page
```

**UI Access**
```bash
open http://localhost:5173  # macOS
xdg-open http://localhost:5173  # Linux
start http://localhost:5173  # Windows PowerShell
```

Should show: "🏠 Household Command Center" with 3 mode tabs.

### Step 5: Run Evaluation

**Via UI**
1. Switch to "SIMULATION" mode
2. Click "Run Evaluation"
3. Switch to "INSIGHTS" mode
4. View results (should show 5+ scenarios, all metrics 0-10 range)

**Via Terminal**
```bash
docker-compose run backend pytest tests/test_brief_evaluation.py -s
```

Expected: "DECISION_FEEDBACK_COMPLETE" printed, `evaluation_results.json` created.

### Step 6: Run Full Test Suite

```bash
docker-compose run backend pytest -q
```

Expected: "269 passed" (or similar count).

### Step 7: Verify Live Mode

**Test Brief Endpoint**
```bash
curl http://localhost:8000/brief/default_household
```

Returns:
```json
{
  "status": "success",
  "brief": {...},
  "generated_at": "2025-04-18T08:45:00Z"
}
```

**In UI**
- Switch to "LIVE" mode
- Click "Refresh Brief"
- Should display: brief summary, priority timeline, conflicts, member tasks

### Troubleshooting Restoration

**"docker-compose: command not found"**
- Use `docker compose` (newer syntax) instead
- Or install Docker Compose v2 plugin

**"Port 8000 already in use"**
```bash
# Kill existing containers
docker-compose down
docker ps -a  # Verify

# Try again
docker-compose up --build
```

**"Cannot reach http://localhost:5173"**
- Verify UI container is running: `docker-compose ps`
- Check logs: `docker-compose logs ui`
- Ensure no firewall blocking

**"Evaluation runs but shows no results"**
- Verify pytest installed: `docker-compose run backend pip list | grep pytest`
- Check disk space: `docker-compose run backend df -h /`
- Re-run: `docker-compose run backend pytest tests/test_brief_evaluation.py -s`

**Database "locked" errors**
```bash
# SQLite sometimes locks—restart containers
docker-compose restart backend
```

**UI shows 404 on API calls**
- Verify backend container running: `docker-compose ps`
- Check VITE_BACKEND_URL matches in compose file
- Rebuild UI: `docker-compose up --build ui`

---

## Quick Reference

### Essential Commands

```bash
# Start everything
docker-compose up --build

# Stop everything
docker-compose down

# Run evaluation
docker-compose run backend pytest tests/test_brief_evaluation.py -s

# Run all tests
docker-compose run backend pytest -q

# View logs
docker-compose logs -f backend
docker-compose logs -f ui

# Execute command in backend
docker-compose exec backend python -m pytest --version

# Clean database
docker-compose exec backend rm data/family_orchestration.db

# Rebuild only UI
docker-compose up --build ui
```

### File Locations

```
Family Orchestration Bot/
├── README.md
├── Makefile (make up, make down, make test, make eval)
├── requirements.txt (Python dependencies)
├── docker-compose.yml (service definitions)
├── Dockerfile.backend (FastAPI image)
├── .env (Google API credentials—optional)
│
├── apps/api/
│   ├── main.py (app factory)
│   ├── endpoints/ (brief_endpoint, evaluation_router, etc.)
│   ├── core/ (database, event_bus, etc.)
│   ├── models/ (SQLAlchemy ORM)
│   └── services/ (business logic)
│
├── integration_core/
│   ├── orchestrator.py (main orchestration)
│   ├── state_builder.py
│   ├── brief_builder.py
│   ├── decision_engine.py
│   └── providers.py
│
├── tests/
│   ├── conftest.py (pytest fixtures)
│   ├── test_brief_evaluation.py (main eval test)
│   ├── test_brief_builder.py
│   ├── evaluation/ (scenario engine, scoring, feedback)
│   └── ... (250+ other test files)
│
├── ui/
│   ├── src/
│   │   ├── App.jsx (mode router)
│   │   ├── components/
│   │   │   ├── core/ (TopNav, ModeSelector)
│   │   │   ├── live/ (LIVE mode components)
│   │   │   ├── simulation/ (SIMULATION mode)
│   │   │   └── insights/ (INSIGHTS mode)
│   │   ├── index.css (styling)
│   │   └── main.jsx
│   ├── package.json
│   ├── vite.config.js
│   └── Dockerfile.ui
│
├── evaluation_results.json (latest artifact)
├── simulation_results.json (latest simulation artifact)
├── operational_mode_report.json (operational layer verification artifact)
├── insight_report.json (read-only insight bridge artifact)
├── policy_memory.json (persistent policy-owned household memory snapshot)
├── policy_engine_report.json (policy and itinerary recommendation artifact)
├── assistant_core_report.json (assistant planning and approval-layer verification artifact)
└── data/
    └── family_orchestration.db (SQLite)
```

---

## Known Issues & Risks

### Current Limitations

1. **Mock Providers**
   - Calendar, task, preference providers use mocks
   - Real Google Calendar integration requires OAuth setup
   - Evaluation runs against synthetic household data

2. **Evaluation Drift**
   - If ScoringEngine thresholds change, historical comparisons become unreliable
   - Document any scoring logic changes in CHANGELOG

3. **Scenario Constraints**
   - Only 5 deterministic scenarios currently
   - Doesn't cover edge cases (holidays, sick days, etc.)
   - Consider expanding scenario library for production

4. **UI Limitations**
   - Live mode requires hardcoded household_id ("default_household")
   - No authentication layer
   - Multi-user sync not yet implemented

### Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Evaluation regressions | Run full suite before merging changes |
| Database lock | Use SQLite with proper connection pooling |
| Port conflicts | Use Makefile or docker-compose with custom ports |
| State pollution | conftest.py ensures database/event bus reset per test |
| Evaluation artifact loss | Commit evaluation_results.json to git for tracking |

---

## Summary

This system combines:
- **Production Orchestration**: Real-time household brief generation via REST API
- **Deterministic Evaluation**: Pytest-based synthetic scenario testing with 8-dimension scoring
- **Read-Only Insight Bridge**: Cross-artifact analysis layer for patterns, health summaries, and recommendations
- **Policy + Memory Engine**: Persistent household memory plus suggestion-only policy and itinerary recommendations
- **Household State Manager Layer**: Single user-facing source of truth for assistant reasoning, persistent state history, and deterministic next-action selection
- **Assistant Runtime Compatibility Layer**: Preserves internal runtime plans for daily-loop generation while delegating user-facing reasoning to the household-state manager
- **Feedback Intelligence**: Automatic failure pattern→gap→recommendation pipeline
- **Modern UI**: React/Vite dashboard with a simplified Today State View for assistant actions while keeping other backend layers available additively
- **Containerization**: Single-command Docker Compose startup

It is production-ready, fully testable, and designed for reproducibility across machines.

To restore on a new machine: **Clone → Docker Compose up → Done.**

---

## Documentation Maintenance

This README is maintained as a living system document and is updated as new product surfaces land.

Current as of 2026-04-19:
- Operational product layer endpoints and strict response contract are documented.
- Operational UI tab behavior is documented.
- Operational validation artifact (`operational_mode_report.json`) is tracked in this guide.
- Insight Feedback Bridge endpoints, UI tab, tests, and `insight_report.json` artifact are documented.
- Policy engine endpoints, Home Assistant UI tab, memory snapshot, and `policy_engine_report.json` artifact are documented.
- Household state manager architecture, Today State View, single-action approval flow, `/assistant/run`, `/assistant/query`, `/assistant/approve`, `test_assistant_core.py`, `test_assistant_runtime.py`, and `test_household_state_manager.py` are documented.
