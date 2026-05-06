# HPAL Frontend Control Surface

🎯 **Strict Projection Consumer** — A React-based UI layer that consumes HPAL backend state through read-only projections and write-through HPAL command APIs.

## Overview

This frontend is built on the principle of **projection consumption**: the UI never directly mutates backend state or bypasses HPAL APIs. All state updates flow through:

1. **HPAL Read APIs** — Load family, plans, tasks, events, watermark
2. **Zustand Store** — replace-by-version semantics, watermark guards
3. **Polling Sync Hook** — Periodic refreshes with exponential backoff
4. **React Components** — Display projections with explain panels

### Key Constraints

- ✅ No direct database access
- ✅ No internal orchestration concepts exposed (DAG, leases, outbox)
- ✅ All state updates include metadata (reason_code, system_initiated flag, watermark version)
- ✅ Product domain language only (plan, task, event, person — not "entity", "job", "workflow")
- ✅ Watermark versioning prevents stale projection overwrites
- ✅ All changes visible via explain panel (transparency by default)

## Directory Structure

```
hpal-frontend/
├── src/
│   ├── main.tsx              # Entry point
│   ├── App.tsx               # Root with routing
│   ├── types/
│   │   └── index.ts          # Domain types (FamilyModel, PlanModel, TaskModel, etc.)
│   ├── api/
│   │   └── hpal-client.ts    # Singleton HPAL API client
│   ├── store/
│   │   └── hpal-store.ts     # Zustand store with replace-by-version
│   ├── hooks/
│   │   └── useSyncProjection.ts  # Polling sync layer
│   ├── components/
│   │   ├── index.tsx         # Shared UI components
│   │   ├── Navigation.tsx     # Top nav bar
│   │   └── ErrorBoundary.tsx  # Error handling
│   ├── pages/
│   │   ├── FamilyDashboard.tsx    # Home view
│   │   ├── PlanDetail.tsx         # Plan metadata + tasks
│   │   ├── TaskBoard.tsx          # Kanban board
│   │   ├── CalendarView.tsx       # Events by time window
│   │   └── SystemExplainPanel.tsx # Projection metadata
│   └── styles/
│       ├── global.css        # Base styles
│       ├── navigation.css     # Nav styles
│       ├── error-boundary.css # Error page
│       └── system-explain.css # System panel styles
├── package.json
├── tsconfig.json
├── vite.config.ts
├── index.html
├── .env.example
└── README.md
```

## Pages

### 1. Family Dashboard (Home)
- 📊 Health metrics (stability, conflicts, accuracy)
- 📋 Active plans list
- 📅 Today's tasks and events
- 🚨 Conflicts panel (read-only)
- **Access:** `/`

### 2. Plan Detail
- 📝 Plan metadata (title, status, stability)
- 🔗 Linked tasks (grouped by status)
- 🔄 Recompute status and triggers
- 📈 Revision history timeline
- **Access:** `/plans/:planId`

### 3. Task Board
- 🎯 Kanban board (pending → in_progress → completed / failed)
- 🔍 Filters (by plan, by person)
- 💼 Task cards with priority and status
- **Access:** `/tasks`

### 4. Calendar View
- 📅 Time-block view (4 blocks per day)
- 🔗 Linked plans per event
- 👤 Event source badges (manual vs. system-generated)
- 📍 Participants list
- **Access:** `/calendar`

### 5. System Explain Panel
- 🔍 Projection watermark info
- 📊 Orchestration metrics
- 🛡️ System stability assessment
- ⚡ Propagation latency
- 📖 Help & guidance
- **Access:** `/system`

## Setup & Installation

### Prerequisites

- **Node.js** 18+ and **npm** 9+
- **HPAL Backend** running at `http://localhost:8000` (or configured via `VITE_API_BASE_URL`)

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Create a `.env` file from `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` if your HPAL backend is not at the default location:

```env
VITE_API_BASE_URL=http://localhost:8000
```

### 3. Start Development Server

```bash
npm run dev
```

The app opens at `http://localhost:5173`.

## Development

### Build

```bash
npm run build
```

Output: `dist/` directory with optimized bundle.

### Type Checking

```bash
npm run type-check
```

Validates TypeScript without building.

### Linting (Optional)

```bash
npm run lint
```

## Architecture Principles

### 1. Projection Consumer Pattern

The frontend is a **read-only consumer** of HPAL backend projections:

```
HPAL Backend
    ↓
[Read APIs]
    ↓
Zustand Store (replace-by-version)
    ↓
React Components (display only)
```

All mutations flow through:

```
User Action
    ↓
HPAL Command API
    ↓
Backend Executes
    ↓
Projection Updated
    ↓
Frontend Syncs
```

### 2. Watermark Versioning

Every projection update includes a `ProjectionWatermark`:

```typescript
ProjectionWatermark {
  projection_epoch: number      // Sequence counter
  projection_version: string    // UUID of update
  last_projection_at: ISO8601   // Timestamp
  projection_lag_ms: number     // How stale (ms)
  stale_projection: boolean     // Is current valid?
}
```

The store **prevents version regression**:

```typescript
if (newWatermark.projection_epoch < currentWatermark.projection_epoch) {
  // Ignore stale update
  return;
}
```

### 3. State Discipline (Replace-by-Version)

No partial merges—entire projections are **replaced atomically**:

```typescript
// ❌ Wrong: patch merge
dispatch({ type: "UPDATE_PLAN", planId, changes: { status: "done" } });

// ✅ Right: replace by version
dispatch({
  type: "SET_PLAN_PROJECTION",
  data: entireUpdatedPlan,
  version: watermark.projection_version
});
```

### 4. Polling Strategy

Adaptive sync intervals based on update frequency:

- **Family Overview:** every 30s (low-frequency)
- **Plans/Tasks:** every 15s (medium-frequency)
- **Exponential backoff** on error (max 60s)
- **Stop on stale watermark** (prevents regression)

### 5. Explainability (Mandatory)

Every state change visible via **ExplainPanel**:

```typescript
<ExplainPanel
  reason={event.reason_code}        // "user_triggered", "system_optimized", etc.
  isSystemInitiated={event.system_initiated}
  watermarkVersion={watermark.projection_version}
  lastUpdateTime={watermark.last_projection_at}
/>
```

Users can always understand **why** a change occurred.

## API Integration

### HPAL Client Methods

The `hpalClient` singleton exposes:

#### Read APIs

```typescript
// Family overview (all plans, tasks, events)
getFamilyOverview(familyId: string)
  → FamilyProjection with watermark

// Single plan with linked tasks
getPlanDetail(familyId: string, planId: string)
  → PlanModel with watermark

// Tasks for family or filtered
getTasks(familyId: string, filters?: TaskFilters)
  → TaskModel[] with watermark

// Events for family
getEvents(familyId: string)
  → EventModel[] with watermark
```

#### Write APIs

```typescript
// Create plan (routes through HPAL)
createPlan(familyId: string, request: CreatePlanRequest)
  → PlanModel with watermark

// Update plan
updatePlan(familyId: string, planId: string, request: UpdatePlanRequest)
  → PlanModel with watermark

// Trigger recompute
recomputePlan(familyId: string, planId: string, reason: string)
  → PlanModel with watermark
```

All methods return ```typescript
{
  data: T,
  watermark: ProjectionWatermark
}
```

### Error Handling

```typescript
try {
  const { data, watermark } = await hpalClient.getFamilyOverview(familyId);
  store.setFamilyProjection(data, watermark.projection_version);
} catch (err) {
  if (err.status === 429) {
    // Rate limited – exponential backoff
  } else if (err.status === 404) {
    // Not found
  } else {
    // 500, network error, etc.
  }
}
```

## State Management (Zustand)

### Store Shape

```typescript
{
  familyProjection: FamilyModel | null
  planProjection: PlanModel[] | null
  taskProjection: TaskModel[] | null
  eventProjection: EventModel[] | null
  systemStateSummary: SystemStateSummary | null
  watermark: ProjectionWatermark | null
  
  loading: boolean
  error: string | null
  
  // Actions
  setFamilyProjection(data, version)
  syncProjection(newData, newVersion)
  updatePlan(changes)
  clearError()
}
```

### Replace-by-Version Semantics

```typescript
setFamilyProjection = (data, version) => {
  const current = this.state.watermark;
  
  // Guard against stale updates
  if (current && parseInt(version) < parseInt(current.projection_version)) {
    console.warn("Ignoring stale projection");
    return;
  }
  
  // Atomic replace
  this.setState({
    familyProjection: data,
    watermark: { ...watermark, projection_version: version }
  });
};
```

## Component Patterns

### Using the Sync Hook

```typescript
export const MyComponent: React.FC = () => {
  const { familyProjection, loading, error } = useHPALStore();

  useSyncProjection({
    familyId: "default-family",
    enabled: true,
    pollInterval: 30000, // 30s
  });

  if (loading && !familyProjection) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div>
      {familyProjection?.family_id}
    </div>
  );
};
```

### ExplainPanel Integration

```typescript
<ExplainPanel
  reason={plan.reason_code}
  isSystemInitiated={plan.system_initiated}
  watermarkVersion={watermark.projection_version}
  lastUpdateTime={watermark.last_projection_at}
/>
```

## Deployment

### Build for Production

```bash
npm run build
```

### Deploy to Static Hosting

Option 1: **Azure Static Web Apps**

```bash
# Install Azure CLI
npm install -g @azure/static-web-apps-cli

# Deploy
swa deploy --app-location dist
```

Option 2: **Docker**

```dockerfile
FROM node:18-alpine as builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 80
```

Option 3: **Netlify / Vercel**

Connect repo and point build command to `npm run build`, output to `dist/`.

## Troubleshooting

### "Connection refused" to HPAL backend

Check `VITE_API_BASE_URL` in `.env` and verify HPAL is running:

```bash
curl http://localhost:8000/healthz
```

### State doesn't update

Check browser console for errors. Common issues:

- **Stale watermark:** Frontend rejected older version. Check backend update time.
- **CORS:** Enable CORS on HPAL backend for `http://localhost:5173`.
- **Network:** Check Network tab in DevTools for failed requests.

### Type errors during build

Run type check:

```bash
npm run type-check
```

Verify all HPAL response types match `src/types/index.ts`.

## Contributing

### Code Style

- **TypeScript strict mode** enabled
- **React functional components** with hooks
- **Zustand for state** (no Redux)
- **Tailwind CSS patterns** (or vanilla CSS)

### Before Committing

```bash
npm run type-check
npm run lint  # if eslint configured
```

## License

Part of the Family Orchestration Bot project.

## Contact

For issues or questions about the HPAL frontend, contact the bot development team.
