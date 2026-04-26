## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\.audit_file_manifest.txt

### Overview
- Purpose: Generated deterministic inventory of repository files.
- Function in system: Acts as machine-generated coverage ledger for audit sequencing.

### Line-Level Issues
- Line 9: security/style — manifest includes `.env` path, which signals sensitive config presence and can leak repository secret topology when shared externally; fix: generate a redacted/public manifest variant that masks secret-bearing paths.
- Lines 14-18: performance — manifest includes `.pytest_cache` internals, which are volatile and create churn/noise in deterministic audits; fix: produce a source-only manifest profile excluding cache directories.
- Lines 19+ (first `.venv` at line 19): performance/style — third-party virtualenv files dominate inventory and dilute application audit signal; fix: maintain two manifests (`source` and `full`) and use `source` for code-risk review.

### Security Risks
- Secret-surface disclosure risk via explicit `.env` presence in shared manifest artifacts.

### Performance Issues
- Review overhead amplified by including dependency/cache trees in the same manifest as first-party code.

### Dead Code
- None directly in this file (it is data inventory), but it references many generated artifacts not suitable for code-health review.

### Notes
- File appears generated and deterministic; issue scope is audit ergonomics and exposure, not runtime behavior.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\.copilot-instructions.md

### Overview
- Purpose: Repository-level coding guidance and phase constraints for assistant-driven modifications.
- Function in system: Behavioral policy document influencing implementation decisions.

### Line-Level Issues
- Lines 26-31: architecture/drift — constraints ban AI/LLM integration and multiple subsystems while current repository contains active LLM, ingestion, and orchestration layers; fix: update this section to current phase/state or split into historical vs active instructions.
- Lines 33-36: architecture/drift — “Only the Task module is active” conflicts with existing multi-module runtime; fix: replace with explicit currently active modules and deprecation status.
- Line 43: architecture — mandated flow (`API → SystemEvent → Router → Module Service → SQLite`) no longer reflects observed event-sourcing/runtime pipelines; fix: document actual canonical flow(s) and boundaries.
- Lines 78-89: bug/style — file ends mid fenced code block and appears truncated, creating ambiguous/incomplete policy parsing; fix: close fenced block and complete acceptance criteria.

### Security Risks
- Stale constraints can trigger unsafe edits by suppressing currently required security checks outside the “Task-only” model.

### Performance Issues
- Indirect: incorrect instructions increase rework and audit churn.

### Dead Code
- Policy sections describing obsolete phase behavior are effectively dead guidance.

### Notes
- Confidence reduced where file truncation may hide additional rules.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\.dockerignore

### Overview
- Purpose: Defines files excluded from Docker build context.
- Function in system: Reduces image context size and prevents accidental artifact inclusion.

### Line-Level Issues
- Lines 1-15: security — no explicit `.env` ignore rule; if `.env` exists, it can be sent in build context; fix: add `.env` and `.env.*` exclusions.
- Lines 1-15: security/performance — no explicit exclusion for `data/*.db` and similar local state artifacts; fix: add targeted ignores for local DB/state files used in this repo.

### Security Risks
- Potential secret/config leakage into Docker context due to missing `.env` ignore.

### Performance Issues
- Build context may include unnecessary local runtime state (DB/log artifacts) if not explicitly excluded.

### Dead Code
- None.

### Notes
- Existing entries for `.venv`, caches, and node modules are appropriate.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\.env

### Overview
- Purpose: Local environment configuration for OAuth credentials.
- Function in system: Provides runtime auth client values.

### Line-Level Issues
- Line 2: security/critical — `GOOGLE_CLIENT_SECRET` is stored in plaintext in repository root; fix: rotate secret immediately and move secrets to secure secret storage.
- Line 1: security/high — OAuth client ID present in tracked root `.env`; fix: keep only placeholders in `.env.example` and keep real values untracked.

### Security Risks
- Credential leakage risk with immediate potential account compromise.

### Performance Issues
- None.

### Dead Code
- None.

### Notes
- Must be paired with ignore policy updates to prevent future commits.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\.gitignore

### Overview
- Purpose: Git exclusion list.
- Function in system: Prevents local artifacts and secrets from entering version control.

### Line-Level Issues
- Line 1: security/high — `.env` is not ignored despite plaintext secrets file existing; fix: add `.env` and `.env.*` with allow-list for `.env.example`.
- Lines 1-8: security/perf — generated artifacts (`*_checkpoint.json`, `*_report.json`, `*.jsonl`) are not explicitly excluded; fix: add targeted ignore patterns.

### Security Risks
- High risk of accidental secret commits.

### Performance Issues
- Repo noise and diff churn from generated runtime artifacts.

### Dead Code
- None.

### Notes
- Current ignores are minimal relative to observed artifact footprint.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\assistant_core_report.json

### Overview
- Purpose: Generated run report for assistant core checks.
- Function in system: Captures execution metadata/status and sample payloads.

### Line-Level Issues
- Lines 7-21: security — report includes endpoint topology and scenario metadata; fix: keep under generated-artifact path and exclude from VCS by default.

### Security Risks
- Operational metadata disclosure if shared publicly.

### Performance Issues
- Low direct impact; contributes to audit/log noise when tracked.

### Dead Code
- N/A (artifact data).

### Notes
- Consider retention policy and redaction for published artifacts.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\AUDIT_INDEX_AND_QUICK_REFERENCE.md

### Overview
- Purpose: Comprehensive audit index and quick-reference playbook.
- Function in system: Human guidance document for remediation and operations.

### Line-Level Issues
- Large single document (~440 lines): maintainability — mixes index, runbooks, and remediation notes; fix: split into focused docs with stable TOC links.
- Multiple command blocks: drift risk — commands can stale without verification timestamps; fix: add a last-validated stamp per section.

### Security Risks
- Medium: centralizes operational assumptions and endpoint references.

### Performance Issues
- Indirect: review latency and cognitive load due to mixed scope.

### Dead Code
- Potential stale sections if not continuously validated.

### Notes
- Content is useful but needs stronger structure/validation metadata.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\audit_manifest.txt

### Overview
- Purpose: Primary audit processing manifest consumed by the batch loop.
- Function in system: Ordered source of file paths for deterministic audit traversal.

### Line-Level Issues
- File encoding: tooling risk — UTF-16 with BOM caused binary-style reads/parsing fragility; fix: normalize to UTF-8 text.
- Entire file: scope/noise — includes generated artifacts and environment directories; fix: maintain tiered manifests (`source`, `infra`, `artifacts`).

### Security Risks
- Path disclosure includes sensitive filenames (for example `.env`) that reveal secret-bearing layout.

### Performance Issues
- High audit overhead due to oversized mixed-scope list.

### Dead Code
- N/A.

### Notes
- Deterministic ordering is good; content curation and encoding are primary concerns.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\baseline_test_output.log

### Overview
- Purpose: Baseline startup and route availability test output.
- Function in system: Evidence artifact for boot and API smoke behavior.

### Line-Level Issues
- Log contains full route surface and subsystem status lines: security — internal service map exposed in plain artifact; fix: redact sensitive metadata before retention.
- Warning lines indicate Pydantic v2 migration key drift; fix: align config keys to current framework expectations.

### Security Risks
- Internal topology and behavior leakage.

### Performance Issues
- Startup warning churn can obscure actionable failures.

### Dead Code
- N/A (artifact data).

### Notes
- Keep as ephemeral CI artifact, not persistent repository content.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\boot_smoke_report.json

### Overview
- Purpose: Statistical multi-run boot validation report.
- Function in system: Aggregates deterministic boot and contract checks across randomized runs.

### Line-Level Issues
- Detailed checks include raw `session_token` JWTs; security/high — token leakage risk even in test contexts; fix: redact/hash tokens before serialization.
- Per-run payloads include entity IDs and event watermarks; security/ops — leak behavioral internals; fix: sanitize high-cardinality identifiers in persisted reports.

### Security Risks
- High due to token exposure and detailed auth/session traces.

### Performance Issues
- Large artifact size increases repository bloat and review overhead.

### Dead Code
- N/A.

### Notes
- Determinism result is strong; artifact hygiene is the main concern.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\breakpoint_checkpoint.json

### Overview
- Purpose: Runtime checkpoint metrics for staged execution.
- Function in system: Captures latency, error classes, and throughput counters.

### Line-Level Issues
- Contains extensive execution internals/counters in repository root; security/ops — reveals performance envelope and failure taxonomy; fix: move to transient artifact storage excluded from VCS.

### Security Risks
- Medium from operational telemetry exposure.

### Performance Issues
- Large JSON checkpoint contributes to repository size and slows text-based audit passes.

### Dead Code
- N/A.

### Notes
- Valuable for diagnostics, but should be managed as ephemeral telemetry.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\calibration_log.jsonl

### Overview
- Purpose: Calibration decision log.
- Function in system: Tracks warning decisions and lag metrics.

### Line-Level Issues
- Lines 1-3: reliability — repeated WARN records suggest sustained threshold pressure; fix: define alerting and adaptive backoff policies.
- Lines 1-3: ops hygiene — generated telemetry file appears in root and can be committed; fix: move to runtime artifacts directory and ignore in VCS.

### Security Risks
- Low to medium (contains timing and control behavior details).

### Performance Issues
- Minimal file-size impact, but indicates potential runtime lag risk.

### Dead Code
- N/A.

### Notes
- Keep as rolling log with retention limits.

## FILE: C:\Users\fb002895\Desktop\Personal\Family Orchestration Bot\chaos_checkpoint.json

### Overview
- Purpose: Chaos scenario checkpoint metrics.
- Function in system: Records resilience experiment counters and timings.

### Line-Level Issues
- Root-level persisted chaos telemetry: security/ops — exposes stress behavior and thresholds; fix: store externally and redact sensitive dimensions.
- Large structured checkpoint in repo: maintainability — adds noise to code review surfaces; fix: exclude generated checkpoints from source control.

### Security Risks
- Medium due to resilience-profile disclosure.

### Performance Issues
- Repository bloat and slower local tooling scans.

### Dead Code
- N/A.

### Notes
- Useful for incident learning, best handled outside committed workspace files.

