
# Time Normalization Utility

## Overview

The time normalization utility (`apps/api/ingestion/adapters/time_normalizer.py`) provides lightweight, deterministic conversion of flexible time input formats into ISO 8601 datetime strings. It's designed exclusively for the adapter layer and does **not** modify OS-1, OS-2, or BriefV1 schema.

## Features

### Supported Input Formats

1. **HH:MM format** (24-hour, military time)
   - Examples: `"11:30"`, `"14:45"`, `"00:00"`, `"23:59"`
   - Validated: 0 ≤ hour < 24, 0 ≤ minute < 60

2. **Named time blocks** (with deterministic defaults)
   - `"morning"` → 09:00 (default for morning tasks)
   - `"afternoon"` → 14:00 (default for afternoon tasks)
   - `"evening"` → 18:30 (default for evening tasks)

3. **Natural language aliases**
   - `"after lunch"` → 13:00 (post-lunch activities)
   - `"tonight"` → 19:00 (evening social/personal time)

### Output Format

All normalized output is ISO 8601 datetime string:
```
"2026-04-16T14:30:00"
```

Combined with system date (from brief context):
```python
normalize_time_input("14:30", reference_date=datetime(2026, 4, 16, 12, 0, 0))
# Returns: "2026-04-16T14:30:00"
```

### Determinism

- **Same input + same reference_date → same normalized output** (guaranteed)
- All 55 unit tests validate deterministic behavior across:
  - HH:MM parsing (10 tests)
  - Time block defaults (6 tests)
  - Natural aliases (6 tests)
  - Reference date handling (3 tests)
  - Determinism validation (4 tests)
  - Reverse lookup (9 tests)
  - Alias listing (7 tests)
  - Edge cases (6 tests)
  - Integration scenarios (4 tests)

## API Reference

### `normalize_time_input(user_input, reference_date=None) → str | None`

Normalize flexible time input into ISO 8601 datetime string.

**Parameters:**
- `user_input` (str | None): Raw user input (HH:MM, time block, or alias)
- `reference_date` (datetime | None): Date to combine with normalized time. Defaults to system today.

**Returns:**
- ISO 8601 datetime string on success
- `None` if input invalid/empty

**Example:**
```python
from apps.api.ingestion.adapters.time_normalizer import normalize_time_input
from datetime import datetime

# HH:MM format
normalize_time_input("14:30", reference_date=datetime(2026, 4, 16))
# Returns: "2026-04-16T14:30:00"

# Named time block
normalize_time_input("morning", reference_date=datetime(2026, 4, 16))
# Returns: "2026-04-16T09:00:00"

# Natural alias
normalize_time_input("after lunch", reference_date=datetime(2026, 4, 16))
# Returns: "2026-04-16T13:00:00"

# Case-insensitive
normalize_time_input("AFTERNOON", reference_date=datetime(2026, 4, 16))
# Returns: "2026-04-16T14:00:00"

# With spaces
normalize_time_input("  tonight  ", reference_date=datetime(2026, 4, 16))
# Returns: "2026-04-16T19:00:00"

# Invalid/empty
normalize_time_input(None)
# Returns: None

normalize_time_input("25:90")  # Invalid hour/minute
# Returns: None
```

### `get_time_block_from_iso(iso_datetime_str) → str | None`

Reverse-lookup: given an ISO datetime string, return the time block or alias it belongs to.

**Parameters:**
- `iso_datetime_str` (str | None): ISO 8601 datetime string

**Returns:**
- Time block/alias name if recognized ("morning", "afternoon", "evening", "after lunch", "tonight")
- `None` if unrecognized or invalid

**Example:**
```python
from apps.api.ingestion.adapters.time_normalizer import get_time_block_from_iso

get_time_block_from_iso("2026-04-16T09:00:00")
# Returns: "morning"

get_time_block_from_iso("2026-04-16T13:00:00")
# Returns: "after lunch"

get_time_block_from_iso("2026-04-16T11:30:00")
# Returns: None (non-standard time)
```

### `list_time_aliases() → dict[str, str]`

Return all supported time aliases and their normalized times.

**Returns:**
- Dictionary mapping alias names to time strings (HH:MM format)

**Example:**
```python
from apps.api.ingestion.adapters.time_normalizer import list_time_aliases

list_time_aliases()
# Returns:
# {
#   'after lunch': '13:00',
#   'tonight': '19:00',
#   'morning': '09:00',
#   'afternoon': '14:00',
#   'evening': '18:30'
# }
```

## Integration: Manual Items with BriefV1

The time normalizer is integrated into `brief_endpoint.py`'s `map_manual_to_brief()` function:

```python
def map_manual_to_brief(
    base_brief: dict[str, Any], 
    manual_items: list[dict[str, Any]]
) -> dict[str, Any]:
    # ...
    for item in manual_items:
        action = {"title": str(item.get("title", "")).strip()}
        
        raw_time = item.get("time")
        if raw_time:
            # Preserve raw input for traceability
            action["raw_time_input"] = str(raw_time).strip()
            
            # Normalize using time_normalizer
            normalized_iso = normalize_time_input(
                raw_time, 
                reference_date=reference_date
            )
            
            if normalized_iso:
                action["start_time"] = normalized_iso
                base_brief["scheduled_actions"].append(action)
            else:
                # Treat as unscheduled if normalization fails
                base_brief["unscheduled_actions"].append(action)
        else:
            base_brief["unscheduled_actions"].append(action)
    
    return base_brief
```

**Key behaviors:**
1. **Raw input preserved**: `raw_time_input` field always contains original user input
2. **Normalized time stored**: `start_time` field contains ISO 8601 result (if successful)
3. **Graceful fallback**: If normalization fails, item moves to `unscheduled_actions`
4. **BriefV1 compatible**: Output maintains v1-required keys (priorities, warnings, risks, summary)

## Usage Examples

### Example 1: User submits flexible task entry

```python
# User submits via /add endpoint
{
    "title": "Team meeting",
    "type": "event",
    "time": "afternoon"
}

# In map_manual_to_brief():
# - raw_time_input = "afternoon"
# - normalize_time_input("afternoon") = "2026-04-16T14:00:00"
# - Action added to scheduled_actions

# Resulting scheduled_action:
{
    "title": "Team meeting",
    "raw_time_input": "afternoon",
    "start_time": "2026-04-16T14:00:00"
}

# Renderer then displays:
# "Afternoon
# - 2:00 PM | Team meeting"
```

### Example 2: Diagnostic/audit trail

```python
# To understand what user entered (despite normalization):
action = {
    "title": "Lunch with Jane",
    "raw_time_input": "after lunch",      # <-- Original user input
    "start_time": "2026-04-16T13:00:00"
}

# Audit trail can show: "User entered 'after lunch', system normalized to 13:00"
```

### Example 3: Handling invalid times

```python
{
    "title": "Doctor appt",
    "type": "event",
    "time": "invalid time"  # Bad input
}

# In map_manual_to_brief():
# - normalize_time_input("invalid time") = None
# - Item moved to unscheduled_actions (graceful fallback)

# Result:
{
    "title": "Doctor appt",
    "raw_time_input": "invalid time",
    # No start_time field
}

# Renderer displays:
# "Unscheduled / Deferred
# - Doctor appt"
```

## Testing

### Unit Tests

Located in `tests/test_time_normalization.py` (55 tests):
- All basic format parsing (HH:MM, time blocks, aliases)
- Case-insensitivity and whitespace handling
- Reference date behavior
- Determinism validation
- Reverse lookup
- Alias listing
- Edge cases and error conditions
- Integration with manual items

**Run tests:**
```bash
pytest tests/test_time_normalization.py -v
# All 55 tests pass
```

### Integration Tests

Located in `tests/test_http_brief.py`:
- `test_time_normalization_normalizes_flexible_formats()`: Validates HH:MM, "morning", "after lunch", "tonight" normalization
- `test_time_normalization_preserves_raw_input()`: Confirms `raw_time_input` field preservation across all formats

**Run tests:**
```bash
pytest tests/test_http_brief.py -v
# All 6 tests pass, including 2 time normalization integration tests
```

## Design Principles

1. **Adapter-layer only**: Time normalizer lives exclusively in `/ingestion/adapters/` layer
2. **No OS mutation**: Does not modify OS-1 event ingestion, OS-2 decision engine, or BriefV1 schema
3. **Raw input preservation**: Original user input always tracked for audit/traceability
4. **Deterministic**: Same input + same reference_date → identical output (testable, reproducible)
5. **Graceful degradation**: Invalid times handled safely (fallback to unscheduled)
6. **Case/whitespace tolerant**: User-friendly parsing (uppercase, lowercase, spaces all work)
7. **Reference date coupling**: Normalized times always tied to brief context date (not current time)

## Limitations & Non-Features

- **No time zones**: All times are naive (local to brief context date)
- **No relative times**: Cannot handle "in 2 hours", "next Tuesday", etc.
- **No recurring patterns**: Single-shot normalization (no "every Monday")
- **No duration**: Only start_time supported (not end_time or duration)
- **5 fixed aliases**: Predefined set; cannot dynamically add new aliases
- **No seconds precision**: Fixed at HH:MM granularity (no :SS)

## Future Options

If in future you want to extend:

1. **Add more aliases**:
   ```python
   _TIME_ALIASES["breakfast"] = time(8, 0)
   _TIME_ALIASES["lunch"] = time(12, 0)
   _TIME_ALIASES["dinner"] = time(18, 0)
   ```

2. **Add timezone support**:
   ```python
   normalize_time_input(user_input, reference_date, timezone="US/Eastern")
   ```

3. **Add duration support**:
   ```python
   normalize_time_range(user_input, reference_date)
   # Returns: {"start_time": "...", "end_time": "...", "duration_minutes": 60}
   ```

4. **Add relative time parsing**:
   ```python
   normalize_time_input("tomorrow at 3pm", reference_date)
   normalize_time_input("next Monday morning", reference_date)
   ```

All would be additive, requiring no changes to existing code.

## Summary

✅ **Completed**:
- Time normalizer utility with 5 input format types
- Deterministic mapping (same input → same output)
- ISO 8601 output format
- Raw input preservation for traceability
- BriefV1-compatible integration
- 55 unit tests (all passing)
- 2 integration tests (all passing)
- Graceful fallback for invalid times
- Reverse lookup and alias listing utilities
- Comprehensive documentation

✅ **Not Modified**:
- OS-1 event ingestion logic
- OS-2 decision engine behavior
- BriefV1 schema structure
- Core brief generation pipeline
- Renderer logic

✅ **Tested**:
- 61 total tests passing (55 unit + 6 integration)
- 149 total codebase tests passing (unrelated flaky test pre-exists)
