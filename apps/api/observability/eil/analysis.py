"""
EIL Analysis Layer
------------------
Stateless, re-runnable analysis of stored trace data.

This module NEVER modifies stored traces.  Feed it a list of trace dicts and
get back structured maps.  Run it as often as needed without side effects.

Public API:
    build_execution_map(traces)          → dict with functions + reachability
    build_module_heatmap(execution_map)  → dict aggregated by module
    render_execution_map_markdown(...)   → str (EXECUTION_MAP.md)
    detect_regression(baseline, current) → RegressionReport
    load_all_traces(storage, extra_dirs) → list[dict]  (convenience loader)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from apps.api.observability.eil.storage import StorageBackend

from apps.api.observability.eil.tracer import TRACE_TARGETS, get_instrumented_functions


# ---------------------------------------------------------------------------
# Temperature thresholds
# ---------------------------------------------------------------------------
def _classify(count: int, hot_cutoff: int, warm_cutoff: int) -> str:
    if count == 0:
        return "DEAD"
    if count >= hot_cutoff:
        return "HOT"
    if count >= warm_cutoff:
        return "WARM"
    return "COLD"


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------
def build_execution_map(
    traces: list[dict[str, Any]],
    *,
    extra_targets: set[str] | None = None,
) -> dict[str, Any]:
    """Produce a function-level execution map from a list of trace dicts.

    Args:
        traces: List of serialised TraceSession dicts (from any storage backend
                or loaded source files).
        extra_targets: Additional functions to always include in the map even if
                       never called.  Merged with TRACE_TARGETS.

    Returns:
        {
            "trace_count": int,
            "functions": {
                "<module>.<qualname>": {
                    "execution_count": int,
                    "unique_callers": [str, ...],
                    "temperature": "HOT" | "WARM" | "COLD" | "DEAD",
                },
                ...
            },
            "entrypoint_reachability": {
                "<entrypoint>": [<function_key>, ...],
                ...
            },
        }
    """
    all_targets = TRACE_TARGETS | (extra_targets or set()) | get_instrumented_functions()

    call_counts: dict[str, int] = {}
    callers: dict[str, set[str]] = {}
    entrypoint_reachability: dict[str, set[str]] = {}

    for trace in traces:
        entrypoint = str(trace.get("entrypoint", "unknown"))
        events = trace.get("events", [])
        if not isinstance(events, list):
            continue

        stack: list[str] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            event_type = str(event.get("event_type", ""))
            module = str(event.get("module", ""))
            function = str(event.get("function", ""))
            if not module or not function:
                continue
            function_key = f"{module}.{function}"

            if event_type == "function_entry":
                call_counts[function_key] = call_counts.get(function_key, 0) + 1
                entrypoint_reachability.setdefault(entrypoint, set()).add(function_key)
                if stack:
                    callers.setdefault(function_key, set()).add(stack[-1])
                stack.append(function_key)
            elif event_type in {"function_exit", "function_error"}:
                if stack:
                    stack.pop()

    all_functions = sorted(all_targets | set(call_counts.keys()))
    max_count = max(call_counts.values(), default=0)
    hot_cutoff = max(10, int(max_count * 0.6)) if max_count else 0
    warm_cutoff = max(3, int(max_count * 0.2)) if max_count else 0

    summary: dict[str, dict[str, Any]] = {}
    for function_key in all_functions:
        count = call_counts.get(function_key, 0)
        summary[function_key] = {
            "execution_count": count,
            "unique_callers": sorted(callers.get(function_key, set())),
            "temperature": _classify(count, hot_cutoff, warm_cutoff),
        }

    return {
        "trace_count": len(traces),
        "functions": summary,
        "entrypoint_reachability": {
            k: sorted(v) for k, v in sorted(entrypoint_reachability.items())
        },
    }


def build_module_heatmap(execution_map: dict[str, Any]) -> dict[str, Any]:
    """Aggregate function-level data up to module level.

    Returns:
        {
            "<module>": {
                "total_calls": int,
                "function_count": int,
                "hot_count": int,
                "warm_count": int,
                "cold_count": int,
                "dead_count": int,
                "temperature": "HOT" | "WARM" | "COLD" | "DEAD",
            },
            ...
        }
    """
    module_data: dict[str, dict[str, Any]] = {}

    for function_key, meta in execution_map.get("functions", {}).items():
        # Derive module from function key: everything before the last segment
        # that is a qualname (contains class.method pattern)
        parts = function_key.rsplit(".", 1)
        module_name = parts[0] if len(parts) == 2 else function_key

        entry = module_data.setdefault(
            module_name,
            {"total_calls": 0, "function_count": 0,
             "hot_count": 0, "warm_count": 0, "cold_count": 0, "dead_count": 0},
        )
        count = int(meta.get("execution_count", 0))
        temp = str(meta.get("temperature", "DEAD"))
        entry["total_calls"] += count
        entry["function_count"] += 1
        entry[f"{temp.lower()}_count"] += 1

    # Assign module temperature from highest-temperature function it contains
    temp_rank = {"HOT": 4, "WARM": 3, "COLD": 2, "DEAD": 1}
    for module_name, entry in module_data.items():
        for temp in ("hot", "warm", "cold", "dead"):
            if entry[f"{temp}_count"] > 0:
                entry["temperature"] = temp.upper()
                break
        else:
            entry["temperature"] = "DEAD"

    return dict(sorted(module_data.items()))


# ---------------------------------------------------------------------------
# Regression detection
# ---------------------------------------------------------------------------
@dataclass
class RegressionReport:
    new_dead_functions: list[str] = field(default_factory=list)
    temperature_regressions: list[dict[str, str]] = field(default_factory=list)
    new_functions: list[str] = field(default_factory=list)
    has_regression: bool = False

    def summary(self) -> str:
        lines = []
        if self.has_regression:
            lines.append("REGRESSION DETECTED")
        else:
            lines.append("No regressions detected.")
        if self.new_dead_functions:
            lines.append(f"  New DEAD functions ({len(self.new_dead_functions)}):")
            for fn in self.new_dead_functions:
                lines.append(f"    - {fn}")
        if self.temperature_regressions:
            lines.append(f"  Temperature regressions ({len(self.temperature_regressions)}):")
            for item in self.temperature_regressions:
                lines.append(f"    - {item['function']}: {item['before']} → {item['after']}")
        if self.new_functions:
            lines.append(f"  New functions seen ({len(self.new_functions)}):")
            for fn in self.new_functions:
                lines.append(f"    + {fn}")
        return "\n".join(lines)


_TEMP_RANK = {"DEAD": 0, "COLD": 1, "WARM": 2, "HOT": 3}


def detect_regression(
    baseline_map: dict[str, Any],
    current_map: dict[str, Any],
) -> RegressionReport:
    """Compare two execution maps to detect regressions.

    A regression is:
    - A function that was WARM or HOT in baseline but is now DEAD.
    - A function whose temperature dropped by more than one level.

    New functions (not in baseline) are flagged as informational only.
    """
    baseline_fns: dict[str, dict[str, Any]] = baseline_map.get("functions", {})
    current_fns: dict[str, dict[str, Any]] = current_map.get("functions", {})

    report = RegressionReport()

    for fn, baseline_meta in baseline_fns.items():
        before_temp = str(baseline_meta.get("temperature", "DEAD"))
        current_meta = current_fns.get(fn)
        if current_meta is None:
            # Function disappeared entirely
            if _TEMP_RANK.get(before_temp, 0) >= _TEMP_RANK["WARM"]:
                report.new_dead_functions.append(fn)
                report.has_regression = True
            continue

        after_temp = str(current_meta.get("temperature", "DEAD"))
        before_rank = _TEMP_RANK.get(before_temp, 0)
        after_rank = _TEMP_RANK.get(after_temp, 0)

        if after_temp == "DEAD" and before_rank >= _TEMP_RANK["WARM"]:
            report.new_dead_functions.append(fn)
            report.has_regression = True
        elif after_rank < before_rank - 1:
            report.temperature_regressions.append(
                {"function": fn, "before": before_temp, "after": after_temp}
            )
            report.has_regression = True

    for fn in current_fns:
        if fn not in baseline_fns:
            report.new_functions.append(fn)

    report.new_dead_functions.sort()
    report.temperature_regressions.sort(key=lambda x: x["function"])
    report.new_functions.sort()
    return report


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------
def render_execution_map_markdown(
    execution_map: dict[str, Any],
    *,
    module_heatmap: dict[str, Any] | None = None,
) -> str:
    trace_count = int(execution_map.get("trace_count", 0))
    functions = execution_map.get("functions", {})
    reachability = execution_map.get("entrypoint_reachability", {})

    lines = [
        "# EXECUTION_MAP",
        "",
        f"Total traces analyzed: {trace_count}",
        "",
        "## Function Heat",
        "",
        "| Function | Count | Unique Callers | Temperature |",
        "| --- | ---: | ---: | --- |",
    ]

    for function_key, meta in sorted(functions.items()):
        count = int(meta.get("execution_count", 0))
        caller_count = len(meta.get("unique_callers", []))
        temperature = str(meta.get("temperature", "DEAD"))
        lines.append(f"| {function_key} | {count} | {caller_count} | {temperature} |")

    # Module heatmap section
    if module_heatmap:
        lines.extend(["", "## Module Heatmap", "",
                       "| Module | Total Calls | Functions | HOT | WARM | COLD | DEAD | Temperature |",
                       "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"])
        for module_name, meta in sorted(module_heatmap.items()):
            lines.append(
                f"| {module_name} "
                f"| {meta['total_calls']} "
                f"| {meta['function_count']} "
                f"| {meta['hot_count']} "
                f"| {meta['warm_count']} "
                f"| {meta['cold_count']} "
                f"| {meta['dead_count']} "
                f"| {meta['temperature']} |"
            )

    lines.extend(["", "## Reachability", ""])
    for entrypoint, reached in sorted(reachability.items()):
        lines.append(f"### {entrypoint}")
        if not reached:
            lines.append("- (no traced functions)")
        else:
            for function_key in reached:
                lines.append(f"- {function_key}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


# ---------------------------------------------------------------------------
# Convenience loader
# ---------------------------------------------------------------------------
def load_all_traces(
    storage: "StorageBackend",
    *,
    extra_source_dirs: list[Path] | None = None,
    filters: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Load traces from the primary storage backend plus optional JSON/JSONL files.

    Args:
        storage: A StorageBackend instance.
        extra_source_dirs: Additional directories to scan for *.json/*.jsonl files.
        filters: Optional filter dict forwarded to storage.load_traces().
    """
    traces = storage.load_traces(filters=filters)

    if extra_source_dirs:
        from apps.api.observability.eil.storage import JSONLStorageBackend
        tmp_loader = JSONLStorageBackend(Path("/dev/null"))  # used only for load_source_file
        for directory in extra_source_dirs:
            if not directory.is_dir():
                continue
            for path in sorted(directory.glob("*.json")) | set(directory.glob("*.jsonl")):
                loaded = tmp_loader.load_source_file(path)
                for item in loaded:
                    item.setdefault("source", path.stem)
                traces.extend(loaded)

    return traces
