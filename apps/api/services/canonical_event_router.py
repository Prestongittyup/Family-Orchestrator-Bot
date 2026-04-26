from __future__ import annotations

from apps.api.core.event_bus import get_event_bus
from apps.api.schemas.canonical_event import CanonicalEventEnvelope, is_registered_event_type
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.services.event_log_service import log_system_event
from apps.api.realtime.broadcaster import broadcaster


class CanonicalEventRouter:
    @staticmethod
    def _emit_to_sse(event: CanonicalEventEnvelope) -> None:
        """Emit to SSE with a router-owned, non-overridable origin marker."""
        # Guardrail: external callers must never pre-mark origin state.
        if hasattr(event, "__origin_router"):
            raise RuntimeError("SSE violation: origin marker override attempt")

        # Router is the only authority that can mark SSE origin.
        object.__setattr__(event, "__origin_router", True)
        broadcaster.publish_sync(event)

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
        self._emit_to_sse(envelope)

        return results


canonical_event_router = CanonicalEventRouter()
