from __future__ import annotations

from archive.apps.api.core.event_bus import get_event_bus
from archive.apps.api.schemas.canonical_event import CanonicalEventEnvelope, is_registered_event_type
from archive.apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from archive.apps.api.services.event_log_service import log_system_event
from archive.apps.api.realtime.broadcaster import broadcaster


class CanonicalEventRouter:
    def route(
        self,
        envelope: CanonicalEventEnvelope,
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> object | None:
        """Route canonical event through system.

        STRICT INVARIANCE:
        - No caller-controlled transport emission flags
        - Broadcaster is the only SSE authority
        """
        # CRITICAL INVARIANCE RULE:
        # Routing layer MUST NOT accept or interpret transport-level intent.
        # All events routed here will be emitted to transport unconditionally.

        if not is_registered_event_type(envelope.event_type):
            raise ValueError(f"Unregistered event_type: {envelope.event_type}")

        system_event = CanonicalEventAdapter.to_system_event(envelope)
        results: object | None = None

        if persist:
            log_system_event(system_event)
        if dispatch:
            results = get_event_bus().publish(system_event)

        # Broadcaster is the sole transport boundary and is always invoked.
        if hasattr(envelope, "__origin_router"):
            raise RuntimeError("SSE violation: origin marker override attempt")
        object.__setattr__(envelope, "__origin_router", True)
        broadcaster.publish_sync(envelope)

        return results


canonical_event_router = CanonicalEventRouter()
