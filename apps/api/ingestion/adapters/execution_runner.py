from __future__ import annotations

import threading
import time
from typing import Any
from typing import Callable

from apps.api.ingestion.adapters.email_integration_service import (
    ingest_polled_email_messages,
    ingest_push_email_messages,
)
from apps.api.ingestion.adapters.email_provider_adapter import EmailProviderAdapter
from apps.api.ingestion.adapters.ingestion_defaults import get_ingestion_execution_config


_rate_limit_lock = threading.RLock()
_rate_limit_events: dict[int, list[float]] = {}


def _build_summary(cycle_result: dict[str, Any]) -> dict[str, int]:
    results = cycle_result.get("results", [])
    processed = 0
    failed = 0

    for row in results:
        outcome = row.get("outcome", {}) if isinstance(row, dict) else {}
        status = outcome.get("status") if isinstance(outcome, dict) else None
        if status == "processed":
            processed += 1
        elif status == "failed":
            failed += 1

    return {
        "total": len(results),
        "processed": processed,
        "failed": failed,
    }


def _is_rate_limited(
    *,
    adapter: EmailProviderAdapter,
    now: float,
    max_cycles: int,
    window_seconds: float,
) -> tuple[bool, float]:
    adapter_key = id(adapter)

    with _rate_limit_lock:
        history = list(_rate_limit_events.get(adapter_key, []))
        min_time = now - window_seconds
        history = [timestamp for timestamp in history if timestamp >= min_time]

        if len(history) >= max_cycles:
            oldest = history[0]
            retry_after = max(0.0, window_seconds - (now - oldest))
            _rate_limit_events[adapter_key] = history
            return True, retry_after

        history.append(now)
        _rate_limit_events[adapter_key] = history
        return False, 0.0


def _run_cycle_once(adapter: EmailProviderAdapter, *, mode: str) -> dict[str, Any]:
    if mode == "poll":
        return ingest_polled_email_messages(adapter)
    if mode == "push":
        return ingest_push_email_messages(adapter)
    raise ValueError(f"Unsupported ingestion mode '{mode}'. Use 'poll' or 'push'.")


def run_email_ingestion_cycle(
    adapter: EmailProviderAdapter,
    *,
    mode: str = "poll",
    profile: str | None = None,
    retry_attempts: int | None = None,
    backoff_seconds: float | None = None,
    max_backoff_seconds: float | None = None,
    rate_limit_max_cycles: int | None = None,
    rate_limit_window_seconds: float | None = None,
    now_fn: Callable[[], float] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """
    Execute one manual ingestion cycle for an external email adapter.

    This runner is scheduler-agnostic and intentionally lightweight.
    It does not start background threads and does not alter OS-1/OS-2 logic.

    Supported modes:
      - "poll": ingest provider polled messages
      - "push": ingest provider queued push messages

    Resilience features (adapter layer only):
      - bounded retry with exponential backoff for adapter/runtime failures
      - per-adapter in-memory rate limiting
    """
    normalized_mode = mode.strip().lower()

    if normalized_mode not in {"poll", "push"}:
        raise ValueError(f"Unsupported ingestion mode '{mode}'. Use 'poll' or 'push'.")

    config = get_ingestion_execution_config(profile=profile)

    attempts = max(1, int(retry_attempts if retry_attempts is not None else config.retry_attempts))
    base_backoff = max(
        0.0,
        float(backoff_seconds if backoff_seconds is not None else config.backoff_seconds),
    )
    cap_backoff = max(
        base_backoff,
        float(max_backoff_seconds if max_backoff_seconds is not None else config.max_backoff_seconds),
    )
    max_cycles = max(
        1,
        int(rate_limit_max_cycles if rate_limit_max_cycles is not None else config.rate_limit_max_cycles),
    )
    window_seconds = max(
        0.001,
        float(
            rate_limit_window_seconds
            if rate_limit_window_seconds is not None
            else config.rate_limit_window_seconds
        ),
    )

    _now = now_fn or time.monotonic
    _sleep = sleep_fn or time.sleep

    now = float(_now())
    limited, retry_after = _is_rate_limited(
        adapter=adapter,
        now=now,
        max_cycles=max_cycles,
        window_seconds=window_seconds,
    )
    if limited:
        return {
            "status": "rate_limited",
            "mode": normalized_mode,
            "profile": config.profile,
            "provider": getattr(adapter, "provider_name", "unknown"),
            "summary": {
                "total": 0,
                "processed": 0,
                "failed": 0,
            },
            "retry_after_seconds": retry_after,
            "cycle_result": {
                "status": "rate_limited",
                "provider": getattr(adapter, "provider_name", "unknown"),
                "mode": normalized_mode,
                "count": 0,
                "results": [],
            },
        }

    last_error: str | None = None
    for attempt in range(1, attempts + 1):
        try:
            cycle_result = _run_cycle_once(adapter, mode=normalized_mode)
            return {
                "status": "ok",
                "mode": normalized_mode,
                "profile": config.profile,
                "provider": getattr(adapter, "provider_name", "unknown"),
                "summary": _build_summary(cycle_result),
                "cycle_result": cycle_result,
                "attempts_used": attempt,
            }
        except Exception as exc:
            last_error = str(exc)
            if attempt >= attempts:
                break

            backoff = min(cap_backoff, base_backoff * (2 ** (attempt - 1)))
            if backoff > 0:
                _sleep(backoff)

    return {
        "status": "failed",
        "mode": normalized_mode,
        "profile": config.profile,
        "provider": getattr(adapter, "provider_name", "unknown"),
        "summary": {
            "total": 0,
            "processed": 0,
            "failed": 0,
        },
        "cycle_result": {
            "status": "failed",
            "provider": getattr(adapter, "provider_name", "unknown"),
            "mode": normalized_mode,
            "count": 0,
            "results": [],
        },
        "attempts_used": attempts,
        "error": {
            "message": last_error or "Unknown adapter execution failure",
        },
    }


def get_ingestion_runtime_status(
    *,
    adapter: EmailProviderAdapter | None = None,
    profile: str | None = None,
) -> dict[str, Any]:
    """
    Read-only diagnostics for adapter execution runtime configuration/state.

    No control logic or mutations are performed here.
    """
    config = get_ingestion_execution_config(profile=profile)

    adapter_info = {
        "provider": getattr(adapter, "provider_name", "unknown") if adapter is not None else None,
        "adapter_class": adapter.__class__.__name__ if adapter is not None else None,
        "sandbox_mode": bool(getattr(adapter, "sandbox_mode", False)) if adapter is not None else None,
    }

    with _rate_limit_lock:
        active_rate_limit_entries = len(_rate_limit_events)
        adapter_event_count = (
            len(_rate_limit_events.get(id(adapter), [])) if adapter is not None else None
        )

    return {
        "active_profile": config.profile,
        "retry_configuration": {
            "retry_attempts": config.retry_attempts,
            "backoff_seconds": config.backoff_seconds,
            "max_backoff_seconds": config.max_backoff_seconds,
        },
        "rate_limit_configuration": {
            "rate_limit_window_seconds": config.rate_limit_window_seconds,
            "rate_limit_max_cycles": config.rate_limit_max_cycles,
        },
        "adapter_status": {
            **adapter_info,
            "rate_limit_state": {
                "active_rate_limit_entries": active_rate_limit_entries,
                "adapter_event_count": adapter_event_count,
            },
        },
    }


def _reset_execution_runner_state_for_tests() -> None:
    with _rate_limit_lock:
        _rate_limit_events.clear()
