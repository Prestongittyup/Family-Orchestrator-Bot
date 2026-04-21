"""
UI Bootstrap cache keyed by family_id + projection_version.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from threading import RLock

from apps.api.ui_bootstrap.models import UIBootstrapResponse


@dataclass(frozen=True)
class CacheEntry:
    key: str
    response: UIBootstrapResponse
    stored_at: datetime


class UIBootstrapCache:
    """Thread-safe in-memory cache for full bootstrap snapshots."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._entries: dict[str, CacheEntry] = {}

    def get(self, key: str) -> UIBootstrapResponse | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            return entry.response

    def set(self, key: str, response: UIBootstrapResponse) -> None:
        with self._lock:
            self._entries[key] = CacheEntry(key=key, response=response, stored_at=datetime.utcnow())

    def invalidate_family(self, family_id: str) -> None:
        with self._lock:
            keys = [k for k in self._entries if k.startswith(f"{family_id}:")]
            for key in keys:
                self._entries.pop(key, None)


DEFAULT_UI_BOOTSTRAP_CACHE = UIBootstrapCache()
