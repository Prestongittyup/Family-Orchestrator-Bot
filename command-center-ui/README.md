# Command Center UI (Thin Viewer)

This is a minimal single-page Command Center that only consumes:

- `GET /home?household_id=...`

It is intentionally not a dashboard. It is optimized for a 5-second decision flow:

- state
- delta
- next action

## Files

- `index.html`
- `app.js`
- `styles.css`

## How to run

1. Start the backend on `http://localhost:8000`.
2. Open `command-center-ui/index.html` in a browser.

No build step is required.

## Configure target household and backend

Edit the top of `app.js`:

- `HOUSEHOLD_ID`
- `API_BASE`

Default values:

- `HOUSEHOLD_ID = "test-household-1"`
- `API_BASE = "http://localhost:8000"`

## Behavior

- Polls `/home` every 5 seconds.
- Compares current payload with previous payload.
- If changed, rerenders all sections.
- If unchanged, shows `No new updates`.
- Displays `Last updated: X seconds ago`.
- Decisions are clickable and log to console only.
- Manual refresh button triggers immediate fetch.

## What good output looks like

- Summary dominates the page and is readable at a glance.
- Needs Your Decision is short, urgent, and unavoidable.
- Actions are plain execution lines requiring no interpretation.
- Calendar only shows today and urgent upcoming context.
- Update status is obvious without extra scanning.

## What failure looks like

- User must read the entire screen to know what to do.
- Summary is vague or low priority visually.
- Decisions feel optional instead of immediate.
- Actions contain unclear language or metadata clutter.
- UI feels like a dashboard instead of a command center.

## Validation checklist

1. Time to Action: user can decide in under 5 seconds.
2. No Scanning Required: user does not need to inspect all sections deeply.
3. Decision Pressure: unresolved decisions feel immediate.
4. Action Clarity: each action is executable without interpretation.
