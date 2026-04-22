#!/usr/bin/env python3
"""
scripts/eil_debug.py
--------------------
Single-request trace inspector for the Execution Intelligence Layer.

Usage:
    python scripts/eil_debug.py --last
    python scripts/eil_debug.py --trace-id trace-<uuid>
    python scripts/eil_debug.py --list
    python scripts/eil_debug.py --entrypoint "api.event_ingest"

Output:
    Human-readable call tree with timing, depth indicators, and error details.

Examples:
    # Inspect the most recent trace
    python scripts/eil_debug.py --last

    # List all stored trace IDs with metadata
    python scripts/eil_debug.py --list

    # Filter list to a specific actor type
    python scripts/eil_debug.py --list --actor system_worker

    # Show full call tree for a trace ID
    python scripts/eil_debug.py --trace-id trace-abc123

    # Show all traces for a given entrypoint
    python scripts/eil_debug.py --entrypoint orchestrator.tick
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.observability.eil import bootstrap_eil, get_storage_backend


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
_TEMP_COLOUR: dict[str, str] = {
    "function_entry": ">>",
    "function_exit": "<<",
    "function_error": "!!",
}

def _iso_to_ms(ts: str) -> float:
    """Parse ISO timestamp to float seconds."""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _duration_ms(start: str, end: str | None) -> str:
    if not end:
        return "?"
    ms = (_iso_to_ms(end) - _iso_to_ms(start)) * 1000
    return f"{ms:.1f}ms"


def _render_call_tree(trace: dict[str, Any]) -> str:
    lines = [
        f"Trace ID  : {trace.get('trace_id', '?')}",
        f"Entrypoint: {trace.get('entrypoint', '?')}",
        f"Actor     : {trace.get('actor_type', '?')}",
        f"Source    : {trace.get('source', '?')}",
        f"Started   : {trace.get('started_at', '?')}",
        f"Ended     : {trace.get('ended_at', '?')}",
        f"Duration  : {_duration_ms(trace.get('started_at', ''), trace.get('ended_at'))}",
        "",
        "Call tree:",
    ]

    events = trace.get("events", [])
    depth_enters: dict[int, str] = {}  # depth → timestamp of entry

    for event in events:
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("event_type", ""))
        module = str(event.get("module", ""))
        function = str(event.get("function", ""))
        depth = int(event.get("depth", 0))
        ts = str(event.get("timestamp", ""))
        status = str(event.get("status", "ok"))
        error_type = event.get("error_type")
        error_msg = event.get("error_message")

        indent = "  " * depth
        symbol = _TEMP_COLOUR.get(event_type, "·")

        if event_type == "function_entry":
            depth_enters[depth] = ts
            lines.append(f"{indent}{symbol} {module}.{function}")
        elif event_type == "function_exit":
            enter_ts = depth_enters.pop(depth, ts)
            dur = _duration_ms(enter_ts, ts)
            lines.append(f"{indent}{symbol} {module}.{function}  [{dur}]")
        elif event_type == "function_error":
            enter_ts = depth_enters.pop(depth, ts)
            dur = _duration_ms(enter_ts, ts)
            err_detail = f"{error_type}: {error_msg}" if error_type else "(unknown error)"
            lines.append(f"{indent}{symbol} {module}.{function}  [{dur}]  ERROR: {err_detail}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_list(backend: Any, actor: str | None, entrypoint: str | None) -> None:
    filters: dict[str, str] = {}
    if actor:
        filters["actor_type"] = actor
    if entrypoint:
        filters["entrypoint"] = entrypoint
    traces = backend.load_traces(filters=filters or None)
    if not traces:
        print("No traces found.")
        return
    print(f"{'Trace ID':<45} {'Actor':<20} {'Entrypoint':<50} {'Started'}")
    print("-" * 140)
    for t in traces:
        print(
            f"{t.get('trace_id', '?'):<45} "
            f"{t.get('actor_type', '?'):<20} "
            f"{t.get('entrypoint', '?'):<50} "
            f"{t.get('started_at', '?')}"
        )


def cmd_last(backend: Any) -> None:
    traces = backend.load_traces()
    if not traces:
        print("No traces found.")
        return
    # Most recent by started_at
    traces_sorted = sorted(traces, key=lambda t: t.get("started_at", ""), reverse=True)
    print(_render_call_tree(traces_sorted[0]))


def cmd_trace_id(backend: Any, trace_id: str) -> None:
    traces = backend.load_traces(trace_id=trace_id)
    if not traces:
        print(f"Trace not found: {trace_id}")
        return
    print(_render_call_tree(traces[0]))


def cmd_entrypoint(backend: Any, entrypoint: str) -> None:
    traces = backend.load_traces(filters={"entrypoint": entrypoint})
    if not traces:
        print(f"No traces found for entrypoint: {entrypoint}")
        return
    print(f"Found {len(traces)} trace(s) for entrypoint '{entrypoint}':\n")
    for trace in traces:
        print(_render_call_tree(trace))
        print()


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EIL single-request trace debugger")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--last", action="store_true", help="Show the most recent trace")
    group.add_argument("--list", action="store_true", help="List all stored trace IDs")
    group.add_argument("--trace-id", metavar="TRACE_ID", help="Inspect a specific trace by ID")
    group.add_argument("--entrypoint", metavar="ENTRYPOINT",
                       help="Show all traces for a given entrypoint")

    parser.add_argument("--actor", metavar="ACTOR", help="Filter --list by actor_type")
    args = parser.parse_args(argv)

    bootstrap_eil()
    backend = get_storage_backend()

    if args.last:
        cmd_last(backend)
    elif args.list:
        cmd_list(backend, actor=args.actor, entrypoint=None)
    elif args.trace_id:
        cmd_trace_id(backend, args.trace_id)
    elif args.entrypoint:
        cmd_entrypoint(backend, args.entrypoint)

    return 0


if __name__ == "__main__":
    sys.exit(main())
