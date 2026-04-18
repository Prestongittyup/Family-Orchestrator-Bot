"""
google_calendar_sandbox_runner.py
----------------------------------
Standalone diagnostic runner for the Google Calendar integration pilot.

PURPOSE
-------
Exercises the full Integration Core pipeline end-to-end for a single user,
using GoogleCalendarRealProvider (or a supplied http_client mock), then
prints rich debug output showing:

  a) Raw Google API events (as returned by provider.fetch_events)
  b) Normalized ExternalEvent objects
  c) Deterministic sorted final output
  d) Per-event debug diff: raw → mapped → event_id derivation

SAFETY CONSTRAINTS
------------------
  - Read-only provider only (GoogleCalendarRealProvider enforces this)
  - No background workers
  - No OS-1 imports
  - No OS-2 imports
  - Passes architecture guard

USAGE (real credentials)
------------------------
  from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
  from apps.api.integration_core.google_calendar_sandbox_runner import GoogleCalendarSandboxRunner

  store = InMemoryOAuthCredentialStore()
  store.save_credentials(OAuthCredential(
      user_id="alice",
      provider_name="google_calendar_real",
      access_token="<real-access-token>",
      refresh_token=None,
  ))
  runner = GoogleCalendarSandboxRunner(user_id="alice", credential_store=store)
  result = runner.run()
  runner.print_report(result)

USAGE (with injected mock HTTP client for testing)
--------------------------------------------------
  runner = GoogleCalendarSandboxRunner(
      user_id="alice",
      credential_store=mock_store,
      http_client=MockHttpClient(...),
  )
  result = runner.run()
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from apps.api.integration_core.credentials import CredentialStore, OAuthCredential
from apps.api.integration_core.google_calendar_provider import (
    PROVIDER_NAME,
    GoogleCalendarRealProvider,
    build_event_id_debug,
    map_google_event_to_raw,
)
from apps.api.integration_core.normalization import (
    ExternalEvent,
    normalize_provider_events,
)
from apps.api.integration_core.orchestrator import IntegrationOrchestrator
from apps.api.integration_core.registry import ProviderRegistry


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventDebugEntry:
    """Per-event debug record: raw → mapped → ExternalEvent → id derivation."""
    google_event_raw: dict[str, Any]          # original item from Google API
    mapped_intermediate: dict[str, Any]       # output of map_google_event_to_raw
    normalized_event: ExternalEvent           # deterministic ExternalEvent
    id_debug: dict[str, Any]                  # field-level derivation log


@dataclass
class SandboxRunResult:
    """Full output of a single GoogleCalendarSandboxRunner.run() invocation."""
    user_id: str
    provider_name: str = PROVIDER_NAME
    raw_events: list[dict[str, Any]] = field(default_factory=list)
    normalized_events: list[ExternalEvent] = field(default_factory=list)
    sorted_final_events: list[ExternalEvent] = field(default_factory=list)
    debug_entries: list[EventDebugEntry] = field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class GoogleCalendarSandboxRunner:
    """
    Diagnostic orchestration runner for the Google Calendar pilot.

    Registers GoogleCalendarRealProvider into an isolated ProviderRegistry
    (separate from any global registry), then drives the full:
      fetch → map → normalize → sort pipeline.

    Parameters
    ----------
    user_id:
        The user whose calendar to fetch.
    credential_store:
        Pre-populated with credentials for ``google_calendar_real``.
    http_client:
        Optional injectable HTTP client (used in tests to avoid live calls).
    max_results:
        Upper bound on events to fetch. Defaults to 50.
    """

    def __init__(
        self,
        *,
        user_id: str,
        credential_store: CredentialStore,
        http_client: Any = None,
        max_results: int = 50,
    ) -> None:
        self._user_id = user_id
        self._credential_store = credential_store
        self._http_client = http_client
        self._max_results = max_results

        # Build a PRIVATE registry so this runner never touches the global one.
        self._registry = ProviderRegistry(credential_store=credential_store)
        provider = GoogleCalendarRealProvider(
            credential_store=credential_store,
            http_client=http_client,
        )
        self._registry.register_provider(PROVIDER_NAME, lambda _cs: provider)
        self._orchestrator = IntegrationOrchestrator(self._registry)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> SandboxRunResult:
        """
        Execute the full diagnostic pipeline.

        Returns a ``SandboxRunResult`` even when an exception is raised —
        ``result.error`` will be non-None in that case.
        """
        result = SandboxRunResult(user_id=self._user_id)

        try:
            # Phase 1: Collect raw intermediate events via orchestrator
            orchestrator_events = self._orchestrator.collect_external_events(
                self._user_id,
                max_results_per_provider=self._max_results,
            )
            # raw_payload on each orchestrator ExternalEvent = the mapped intermediate dict
            raw_intermediates = [evt.raw_payload for evt in orchestrator_events]
            result.raw_events = raw_intermediates

            # Strip the traceability-only _raw_google_event key before hashing so
            # that normalisation IDs are stable and match build_event_id_debug output.
            def _strip(r: dict) -> dict:
                return {k: v for k, v in r.items() if k != "_raw_google_event"}

            clean_intermediates = [_strip(r) for r in raw_intermediates]

            # Phase 2: Normalize to deterministic ExternalEvent objects
            normalized = normalize_provider_events(
                user_id=self._user_id,
                provider_name=PROVIDER_NAME,
                raw_events=clean_intermediates,
                event_type="calendar.event",
            )
            result.normalized_events = list(normalized)

            # Phase 3: Sort for deterministic ordering
            sorted_events = sorted(
                normalized,
                key=lambda e: (e.provider_name, e.timestamp, e.event_id),
            )
            result.sorted_final_events = sorted_events

            # Phase 4: Build per-event debug entries
            for raw_intermediate in raw_intermediates:
                google_raw = raw_intermediate.get("_raw_google_event", raw_intermediate)
                mapped = _strip(raw_intermediate)
                norm = normalize_provider_events(
                    user_id=self._user_id,
                    provider_name=PROVIDER_NAME,
                    raw_events=[mapped],
                    event_type="calendar.event",
                )
                id_debug = build_event_id_debug(
                    user_id=self._user_id,
                    raw=raw_intermediate,
                    mapped=mapped,
                )
                result.debug_entries.append(
                    EventDebugEntry(
                        google_event_raw=dict(google_raw),
                        mapped_intermediate=mapped,
                        normalized_event=norm[0],
                        id_debug=id_debug,
                    )
                )

        except Exception as exc:  # noqa: BLE001
            result.error = f"{type(exc).__name__}: {exc}"

        return result

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    @staticmethod
    def print_report(result: SandboxRunResult, *, indent: int = 2) -> None:
        """
        Print a human-readable diagnostic report to stdout.

        Sections:
          a) Raw Google API events
          b) Normalized ExternalEvent output
          c) Deterministic sorted final output
          d) Per-event debug diff
        """
        _sep = "─" * 72

        def _j(obj: Any) -> str:
            try:
                return json.dumps(obj, indent=indent, default=str)
            except Exception:
                return repr(obj)

        print(_sep)
        print(f"  GOOGLE CALENDAR SANDBOX RUNNER — user_id: {result.user_id}")
        print(_sep)

        if result.error:
            print(f"\n[ERROR] {result.error}\n")
            return

        # ── Section a) Raw events ──────────────────────────────────────
        print(f"\n[a] RAW GOOGLE API EVENTS ({len(result.raw_events)} total)\n")
        for i, raw in enumerate(result.raw_events, 1):
            display = {k: v for k, v in raw.items() if k != "_raw_google_event"}
            print(f"  Event {i}:")
            for line in _j(display).splitlines():
                print(f"    {line}")
            print()

        # ── Section b) Normalized ExternalEvent objects ────────────────
        print(f"\n[b] NORMALIZED ExternalEvent OBJECTS ({len(result.normalized_events)} total)\n")
        for i, evt in enumerate(result.normalized_events, 1):
            print(f"  ExternalEvent {i}:")
            print(f"    event_id      : {evt.event_id}")
            print(f"    user_id       : {evt.user_id}")
            print(f"    provider_name : {evt.provider_name}")
            print(f"    event_type    : {evt.event_type}")
            print(f"    timestamp     : {evt.timestamp}")
            print(f"    payload keys  : {sorted(evt.payload.keys())}")
            print()

        # ── Section c) Deterministic sorted output ─────────────────────
        print(f"\n[c] DETERMINISTIC SORTED FINAL OUTPUT ({len(result.sorted_final_events)} total)\n")
        for pos, evt in enumerate(result.sorted_final_events, 1):
            print(f"  [{pos:02d}] {evt.event_id}  ts={evt.timestamp}")
        print()

        # ── Section d) Per-event debug diff ────────────────────────────
        print(f"\n[d] PER-EVENT DEBUG DIFF\n")
        for i, entry in enumerate(result.debug_entries, 1):
            print(f"  ── Event {i} ──────────────────────────────────────────")
            print(f"  Raw Google event id  : {entry.google_event_raw.get('id', '<none>')}")
            print(f"  Raw summary          : {entry.google_event_raw.get('summary', '<none>')}")
            print(f"  Mapped timestamp     : {entry.mapped_intermediate.get('timestamp', '<none>')}")
            print(f"  Mapped status        : {entry.mapped_intermediate.get('status', '<none>')}")
            print(f"  Normalised event_id  : {entry.normalized_event.event_id}")
            print()
            print("  ID derivation inputs:")
            for k, v in entry.id_debug["input_fields"].items():
                print(f"    {k:<28}: {v}")
            print(f"  ID derivation result : {entry.id_debug['derived_event_id']}")
            assert entry.id_debug["derived_event_id"] == entry.normalized_event.event_id, (
                "BUG: debug ID derivation diverged from normalized event_id"
            )
            print()

        print(_sep)
        print(f"  Run complete. {len(result.sorted_final_events)} events processed.")
        print(_sep)
