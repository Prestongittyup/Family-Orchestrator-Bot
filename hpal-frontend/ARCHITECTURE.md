# HPAL Frontend Architecture

## System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                         HPAL FRONTEND                             │
│                    (React + TypeScript + Zustand)                │
└──────────────────────────────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
            ┌──────────────────┐    ┌──────────────────┐
            │   React Router   │    │   Zustand Store  │
            │   (5 Pages)      │    │  (State + Sync)  │
            └──────────────────┘    └──────────────────┘
                    │                         │
        ┌─────┬─────┼─────┬──────┐           │
        ▼     ▼     ▼     ▼      ▼           ▼
    ┌────┐┌────┐┌────┐┌────┐┌────┐    ┌─────────────┐
    │Dash││Plan││Task││Cale││Syse│    │ API Client  │
    │    ││Dtl ││Brd ││ndar││tem ├───▶│ HPAL V1     │
    └────┘└────┘└────┘└────┘└────┘    └─────────────┘
                                              │
                                    ┌─────────┴─────────┐
                                    ▼                   ▼
                            ┌────────────┐    ┌────────────────┐
                            │Read APIs   │    │Write APIs      │
                            │Overview    │    │CreatePlan      │
                            │Plans/Tasks │    │UpdatePlan      │
                            │Events      │    │RecomputePlan   │
                            └────────────┘    └────────────────┘
                                    │                   │
                                    └─────────┬─────────┘
                                              ▼
                                    ┌─────────────────────┐
                                    │  HPAL Backend       │
                                    │  Orchestration      │
                                    │  (DAG, Leases, etc)│
                                    └─────────────────────┘
```

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ PROJECTION CONSUMPTION PATTERN                                       │
└─────────────────────────────────────────────────────────────────────┘

User Action (Click, Submit Form)
         │
         ▼
    UI Component
         │
         ├─→ Optimistic Update? (No for this app)
         │
         ▼
    HPAL Command API
         │
         ├─→ (e.g., POST /plans)
         │
         ▼
    HPAL Backend Executes
         │
         ├─→ Processes DAG
         ├─→ Updates database
         ├─→ Emits watermark
         │
         ▼
    Projection Updated
         │
    [useSyncProjection Hook]
         │
         ├─→ Polls GET /overview (every 30s)
         ├─→ Checks watermark version
         │
         ▼
    Version Check
         │
         ├─→ New version > current? ✓ Accept
         ├─→ New version < current? ✗ Reject (stale)
         │
         ▼
    Zustand Store (Replace-by-Version)
         │
         ├─→ Replace entire state
         ├─→ Update watermark
         ├─→ Trigger re-render
         │
         ▼
    React Components (Projection Display)
         │
         ├─→ Render with ExplainPanel
         ├─→ Show reason & timestamp
         │
         ▼
    User Sees Updated State
```

## Watermark Versioning Strategy

```
┌─────────────────────────────────────────────────────────────────────┐
│ WATERMARK STRUCTURE                                                  │
└─────────────────────────────────────────────────────────────────────┘

type ProjectionWatermark {
  projection_epoch: number         // Monotonic counter (1, 2, 3, ...)
  projection_version: string       // UUID of specific projection
  last_projection_at: ISO8601      // Timestamp of last update
  projection_lag_ms: number        // How stale is the projection?
  stale_projection: boolean        // Is this projection invalid?
}

┌─────────────────────────────────────────────────────────────────────┐
│ VERSION REGRESSION GUARD                                             │
└─────────────────────────────────────────────────────────────────────┘

Scenario: Network packet arrives out of order

1. Frontend receives watermark 1 (epoch: 10)
   → Store: { epoch: 10, version: "uuid-10" }

2. Network delay: watermark 2 arrives before watermark 3

3. Frontend receives watermark 3 (epoch: 12)
   → epoch 12 > 10 ✓ Accept
   → Store: { epoch: 12, version: "uuid-12" }

4. Late watermark 2 (epoch: 11) arrives

5. Frontend receives watermark 2 (epoch: 11)
   → epoch 11 < 12 ✗ Reject
   → Guard prevents overwrite
   → Store remains: { epoch: 12, version: "uuid-12" }

Result: State consistency maintained despite out-of-order delivery
```

## Component Hierarchy

```
App (React Router)
  │
  ├─ Navigation (Sticky top bar)
  │  └─ NavLinks (Dashboard, Tasks, Calendar, System)
  │
  ├─ Routes
  │  │
  │  ├─ Route: "/" → FamilyDashboard
  │  │  ├─ useSyncProjection (30s poll)
  │  │  ├─ SystemHealthIndicator
  │  │  ├─ PlansList (PlanCard x N)
  │  │  ├─ TasksSummary
  │  │  ├─ EventsSummary
  │  │  └─ ConflictsPanel
  │  │
  │  ├─ Route: "/plans/:planId" → PlanDetail
  │  │  ├─ useSyncProjection (15s poll)
  │  │  ├─ PlanMetadata
  │  │  ├─ LinkedTasksGrid (TaskCard x N)
  │  │  ├─ RecomputeStatus
  │  │  ├─ RevisionTimeline
  │  │  └─ ExplainPanel
  │  │
  │  ├─ Route: "/tasks" → TaskBoard
  │  │  ├─ useSyncProjection (15s poll)
  │  │  ├─ Filters (PlanSelect, PersonSelect)
  │  │  ├─ KanbanBoard
  │  │  │  ├─ Column: Pending
  │  │  │  │  └─ TaskCard x N
  │  │  │  ├─ Column: In Progress
  │  │  │  │  └─ TaskCard x N
  │  │  │  ├─ Column: Completed
  │  │  │  │  └─ TaskCard x N
  │  │  │  └─ Column: Failed
  │  │  │     └─ TaskCard x N
  │  │  └─ ExplainPanel
  │  │
  │  ├─ Route: "/calendar" → CalendarView
  │  │  ├─ useSyncProjection (20s poll)
  │  │  ├─ DatePicker
  │  │  ├─ TimeBlocks (4 blocks per day)
  │  │  │  ├─ TimeBlock 1 (00:00 - 06:00)
  │  │  │  │  └─ EventCard x N
  │  │  │  ├─ TimeBlock 2 (06:00 - 12:00)
  │  │  │  │  └─ EventCard x N
  │  │  │  ├─ TimeBlock 3 (12:00 - 18:00)
  │  │  │  │  └─ EventCard x N
  │  │  │  └─ TimeBlock 4 (18:00 - 24:00)
  │  │  │     └─ EventCard x N
  │  │  └─ Legend (Manual vs System)
  │  │
  │  └─ Route: "/system" → SystemExplainPanel
  │     ├─ useSyncProjection (30s poll)
  │     ├─ SystemHealthCard
  │     ├─ WatermarkCard
  │     ├─ MetricsGrid
  │     ├─ StabilityAssessment
  │     ├─ StalenessInfo
  │     └─ HelpGuidance
  │
  └─ Footer
```

## State Management: Zustand Store

```typescript
┌─────────────────────────────────────────────────────────────────────┐
│ HPAL STORE (Zustand)                                                │
└─────────────────────────────────────────────────────────────────────┘

State:
  familyProjection: FamilyModel | null        ← Full family snapshot
  planProjection: PlanModel[] | null          ← All plans
  taskProjection: TaskModel[] | null          ← All tasks
  eventProjection: EventModel[] | null        ← All events
  systemStateSummary: SystemStateSummary      ← Health metrics
  watermark: ProjectionWatermark | null       ← Version tracker

  loading: boolean                            ← Fetch in progress
  error: string | null                        ← Error message

Actions:
  setFamilyProjection(data, version)          ← Atomic replace
  syncProjection(newData, newVersion)         ← Conditional update
  updatePlan(planId, changes)                 ← Optimistic update
  clearError()                                ← Clear errors

Middleware:
  - Version guard (reject if epoch < current)
  - Immutability (no mutations)
  - Type safety (TypeScript)
```

## API Client: HPAL Integration

```typescript
┌─────────────────────────────────────────────────────────────────────┐
│ HPAL CLIENT (Singleton)                                             │
└─────────────────────────────────────────────────────────────────────┘

Methods:

READ (Projection APIs)
  getFamilyOverview(familyId)
    → { data: FamilyProjection, watermark }
    → Used by: useSyncProjection (main loop)

  getPlanDetail(familyId, planId)
    → { data: PlanModel[], watermark }
    → Used by: PlanDetail page

  getTasks(familyId, filters?)
    → { data: TaskModel[], watermark }
    → Used by: TaskBoard page

  getEvents(familyId)
    → { data: EventModel[], watermark }
    → Used by: CalendarView page

WRITE (Command APIs)
  createPlan(familyId, request: CreatePlanRequest)
    → { data: PlanModel, watermark }
    → Routes through HPAL gateway

  updatePlan(familyId, planId, request: UpdatePlanRequest)
    → { data: PlanModel, watermark }
    → Routes through HPAL gateway

  recomputePlan(familyId, planId, reason: string)
    → { data: PlanModel, watermark }
    → Triggers plan recomputation

Error Handling:
  - Retry on 429 (rate limit) with exponential backoff
  - Propagate 404, 500, network errors to caller
  - Extract and include watermark from all responses
```

## Polling Sync Strategy

```typescript
┌─────────────────────────────────────────────────────────────────────┐
│ POLLING INTERVALS (useSyncProjection)                              │
└─────────────────────────────────────────────────────────────────────┘

High-Priority (frequently changing):
  - Plans/Tasks: 10-15 seconds
  - Rationale: User frequently updates tasks

Medium-Priority (moderate changes):
  - Calendar Events: 20 seconds
  - Rationale: Less frequent, less user interaction

Low-Priority (infrequent changes):
  - Overview Summary: 30 seconds
  - System Status: 30 seconds
  - Rationale: System health is stable

Error Recovery:
  - Exponential backoff: 1s → 2s → 4s → 8s → 16s → 30s
  - Cap at 60s max backoff
  - Reset to base on successful fetch

Memory Management:
  - useEffect cleanup stops polling on unmount
  - AbortController cancels in-flight requests
  - No memory leaks from stale intervals
```

## CSS Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ STYLESHEET ORGANIZATION                                             │
└─────────────────────────────────────────────────────────────────────┘

global.css (600+ lines)
  ├─ :root CSS variables (colors, spacing, fonts, shadows)
  ├─ Base elements (*, html, body, form inputs)
  ├─ Typography (h1-h6, p, a, lists)
  ├─ Layout (flexbox, grid, app structure)
  └─ Utilities (buttons, badges, cards, grids, responsive)

components.css (500+ lines)
  ├─ UI component styles
  │  ├─ SystemHealthIndicator
  │  ├─ ConflictBadge
  │  ├─ WatermarkDisplay
  │  ├─ ExplainPanel
  │  ├─ EventBadge
  │  ├─ ChangeIndicator
  │  ├─ LoadingState
  │  └─ PlanCard, TaskCard variants

pages.css (600+ lines)
  ├─ Dashboard page layout
  ├─ PlanDetail page layout
  ├─ TaskBoard Kanban layout
  └─ Responsive grid adjustments

calendar.css (300+ lines)
  ├─ Time blocks layout
  ├─ Event card styling
  └─ Calendar-specific responsive rules

system-explain.css (250+ lines)
  ├─ Metrics grid
  ├─ Stability visualization
  ├─ Help section styling

navigation.css (150+ lines)
  ├─ Sticky navbar
  ├─ Mobile menu toggle

error-boundary.css (50+ lines)
  └─ Error page presentation

Design Principles:
  - CSS variables for theming (dark/light mode ready)
  - Mobile-first responsive design (@media queries)
  - Semantic class names (.card, .btn, .badge, etc.)
  - No external CSS frameworks (custom built)
```

## Security Considerations

```
┌─────────────────────────────────────────────────────────────────────┐
│ SECURITY ARCHITECTURE                                               │
└─────────────────────────────────────────────────────────────────────┘

Frontend Boundaries:
  ✓ No database credentials stored in code
  ✓ API base URL in .env (not hardcoded)
  ✓ CORS whitelist enforced by HPAL backend
  ✓ No local storage of sensitive data
  ✓ All writes through HPAL gateway (no direct DB access)

Network Security:
  ✓ HTTPS only in production (enforced by deployment)
  ✓ CSRF protection via SameSite cookies
  ✓ API rate limiting on backend (429 handling)

Data Validation:
  ✓ TypeScript type checking (compile-time)
  ✓ Input sanitization (React auto-escapes)
  ✓ Response validation (API response types)
  ✓ Watermark version checks (prevent old data override)

Access Control:
  ✓ Backend enforces authentication/authorization
  ✓ Frontend only displays permitted projections
  ✓ No privilege escalation via frontend
  ✓ Family isolation via familyId parameter

Example Attack Scenario (Prevented):
  Attack: User modifies browser to send false watermark epoch
  Result: Store's version guard rejects update
  Outcome: Stale projection detected, user refreshes

Example Attack Scenario (Prevented):
  Attack: User intercepts API response, modifies plan status
  Result: Watermark version changes, stale detection triggers
  Outcome: Next sync fetches fresh version from backend
```

## Performance Optimization

```
┌─────────────────────────────────────────────────────────────────────┐
│ PERFORMANCE STRATEGY                                                │
└─────────────────────────────────────────────────────────────────────┘

Bundle Size:
  - React 18: ~40 KB
  - Zustand: ~10 KB
  - React Router: ~15 KB
  - Total: ~65 KB gzipped

Rendering:
  - Memoization: React.memo() for cards
  - Selective re-renders: Zustand subscriptions
  - CSS-in-JS: None (vanilla CSS)

Network:
  - Conditional polling (only active page polls)
  - Exponential backoff on error
  - Watermark versioning (skip redundant updates)
  - Response compression (gzip via Vite)

Caching:
  - Browser cache for static assets (CSS, JS)
  - Service Worker (optional future enhancement)
  - No aggressive HTTP caching (projections are live)

Lazy Loading:
  - React Router code splitting (optional)
  - Images (defer off-screen)

Monitoring:
  - Application Insights (optional, see DEPLOYMENT.md)
  - Network tab (DevTools)
  - Performance tab (DevTools)
```

## Deployment Considerations

```
┌─────────────────────────────────────────────────────────────────────┐
│ PRODUCTION CHECKLIST                                                │
└─────────────────────────────────────────────────────────────────────┘

Environment:
  ✓ .env.production set with correct HPAL_API_BASE_URL
  ✓ HTTPS enforced (redirect HTTP → HTTPS)
  ✓ CORS whitelist updated on backend

Build:
  ✓ npm run build succeeds
  ✓ dist/ directory contains optimized assets
  ✓ Source maps disabled (optional, for debugging)

Testing:
  ✓ All 5 pages load
  ✓ Projections sync correctly
  ✓ Error states handled gracefully
  ✓ Mobile responsive design verified

Monitoring:
  ✓ Error logs configured
  ✓ Performance metrics tracked
  ✓ Uptime monitoring enabled

Backup:
  ✓ HPAL backend ensures data durability
  ✓ Frontend is stateless (no backup needed)
```

---

**Architecture Version:** 1.0  
**Last Updated:** 2024  
**Status:** Production Ready
