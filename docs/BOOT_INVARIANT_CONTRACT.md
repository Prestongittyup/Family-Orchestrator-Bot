# Boot Invariant Contract

Date: 2026-04-20
Scope: Deterministic startup correctness for API boot path.

## Contract

A startup is considered valid only when all invariants below are satisfied.

### 1. Database Invariants

- DB connection succeeds (`SELECT 1`).
- Required schema tables exist:
  - `households`
  - `users`
  - `devices`
  - `memberships`
  - `session_tokens`
  - `idempotency_keys`
- Connection pool is initialized and reports status.
- Migration mode is explicit:
  - Current mode: `metadata_create_all` (schema sync via SQLAlchemy metadata).

### 2. Identity Invariants

- `POST /v1/identity/household/create` returns 200 for unique `founder_email`.
- Duplicate `founder_email` returns 400, never 500.
- `POST /v1/identity/bootstrap` returns a valid JWT (three-segment token).

### 3. Auth Invariants

- Invalid bearer token on protected endpoint returns 401.
- Missing bearer token on protected endpoint returns 401.
- Valid bearer token on protected endpoint returns 200.
- Public path policy is enforced:
  - `/v1/identity/household/create`
  - `/v1/identity/bootstrap`
  - `/v1/system/boot-status`
  - `/v1/system/health`

### 4. SSE Invariants

- `GET /v1/realtime/stream` with valid bearer token returns `event: connected`.
- Reconnect with zero-sequence watermark does not force `resync_required`.
- Reconnect with old/gapped watermark returns `resync_required`.

### 5. Observability Invariants

- `GET /v1/system/boot-status` returns live diagnostics from runtime checks.
- Boot logs include on every startup:
  - PID
  - requested port
  - environment config hash
  - sanitized DB URL
  - active router list

## Failure Classes

- `BOOT_FAILURE`: startup cannot reach ready state.
- `CONTRACT_FAILURE`: endpoint behavior diverges from contract.
- `STATE_FAILURE`: DB/schema consistency or state invariants fail.
- `AUTH_FAILURE`: token or middleware contract mismatch.
- `STREAM_FAILURE`: SSE connection/reconnect invariant failure.
