"""
Structured JSON logger for production traceability.

Every log line is a JSON object with fixed envelope fields:
    timestamp, level, event, request_id, household_id, ...extra

Uses the stdlib `logging` module under the hood so existing
log-shipping infrastructure works transparently.
"""
from __future__ import annotations

import json
import logging
import sys
import time
import traceback
from typing import Any

from apps.api.observability.trace_context import get_current_trace_id


# ---------------------------------------------------------------------------
# JSON formatter — converts a LogRecord into a one-line JSON string
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Base envelope
        doc: dict[str, Any] = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "event": record.getMessage(),
        }

        # Lift structured fields attached by our helpers
        for key in ("request_id", "household_id", "error", "error_type", "stack_trace"):
            val = getattr(record, key, None)
            if val is not None:
                doc[key] = val

        # Lift any extra kwargs stored in record.__dict__
        extra = getattr(record, "_extra", None)
        if extra:
            doc.update(extra)

        # Exception info (when using logger.exception)
        if record.exc_info:
            doc["stack_trace"] = self.formatException(record.exc_info)

        return json.dumps(doc, default=str)


# ---------------------------------------------------------------------------
# Logger setup — single named logger for the whole API
# ---------------------------------------------------------------------------

_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_JsonFormatter())

_logger = logging.getLogger("family_bot")
_logger.setLevel(logging.DEBUG)
# Avoid duplicate handlers on reload
if not _logger.handlers:
    _logger.addHandler(_handler)
_logger.propagate = False


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def _build_record(
    level: int,
    event_name: str,
    **fields: Any,
) -> None:
    """Emit a structured log record at the given level."""
    request_id = fields.pop("request_id", None) or get_current_trace_id()
    household_id = fields.pop("household_id", None)
    error = fields.pop("error", None)
    error_type = fields.pop("error_type", None)
    stack = fields.pop("stack_trace", None)

    extra: dict[str, Any] = {
        "request_id": request_id,
        "household_id": household_id,
        "error": error,
        "error_type": error_type,
        "stack_trace": stack,
        "_extra": fields,  # remaining k/v go into the JSON doc
    }
    # Strip None values to keep logs compact
    extra = {k: v for k, v in extra.items() if v is not None}

    record = _logger.makeRecord(
        name=_logger.name,
        level=level,
        fn="",
        lno=0,
        msg=event_name,
        args=(),
        exc_info=None,
    )
    record.__dict__.update(extra)
    _logger.handle(record)


def log_event(event_name: str, **fields: Any) -> None:
    """Emit an INFO-level structured log entry.

    Args:
        event_name: Short snake_case identifier, e.g. "event_broadcast"
        **fields:   Arbitrary metadata (watermark, event_type, …)

    Example::
        log_event("event_broadcast",
                  household_id="abc",
                  watermark="1713607240000-42",
                  event_type="task_created")
    """
    _build_record(logging.INFO, event_name, **fields)


def log_warn(event_name: str, **fields: Any) -> None:
    """Emit a WARNING-level structured log entry."""
    _build_record(logging.WARNING, event_name, **fields)


def log_error(event_name: str, error: Exception | str, **fields: Any) -> None:
    """Emit an ERROR-level structured log entry with exception details.

    Args:
        event_name: Short snake_case identifier, e.g. "broadcast_failed"
        error:      Exception or string describing the error
        **fields:   Additional metadata
    """
    if isinstance(error, Exception):
        fields["error"] = str(error)
        fields["error_type"] = type(error).__name__
        fields["stack_trace"] = traceback.format_exc()
    else:
        fields["error"] = str(error)
    _build_record(logging.ERROR, event_name, **fields)
