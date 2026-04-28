"""
feature_flags.py
----------------
Lightweight feature flag registry for Integration Core.

Flags are read from environment variables at call time (not at import time),
so they can be toggled in tests without restarting the process.

All flags default to *disabled* (False) when the environment variable is
absent or set to any value other than the accepted truthy strings.

Accepted truthy values (case-insensitive): "1", "true", "yes", "on"
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Public flag names
# ---------------------------------------------------------------------------

INTEGRATION_CORE_INGESTION_ENABLED = "INTEGRATION_CORE_INGESTION_ENABLED"

# ---------------------------------------------------------------------------
# Registry of known flags and their defaults
# ---------------------------------------------------------------------------

_KNOWN_FLAGS: dict[str, bool] = {
    INTEGRATION_CORE_INGESTION_ENABLED: False,
}

_TRUTHY = frozenset({"1", "true", "yes", "on"})


def is_enabled(flag: str) -> bool:
    """
    Return whether *flag* is enabled.

    Resolution order:
    1. Environment variable named *flag* (case-insensitive truthy check).
    2. Registered default from ``_KNOWN_FLAGS``.
    3. ``False`` for any unregistered flag (safe default).
    """
    raw = os.environ.get(flag, "").strip().lower()
    if raw:
        return raw in _TRUTHY
    return _KNOWN_FLAGS.get(flag, False)


def flag_default(flag: str) -> bool:
    """Return the registered default for *flag* (``False`` if unknown)."""
    return _KNOWN_FLAGS.get(flag, False)
