"""
Shared System Dependencies Registry

Central location for registering and accessing shared system dependencies.

This module is responsible ONLY for:
  • Instantiating shared services (once per application lifetime)
  • Providing accessor functions (get_*) for those services
  • Dependency wiring (connecting components)

This module is NOT responsible for:
  • Business logic
  • Event routing
  • Domain operations
  • Service orchestration

Pattern: Singleton instances are created at module import time and
reused across all requests. This avoids redundant instantiation and
ensures consistent state sharing.
"""

from __future__ import annotations

from apps.api.services.temporal_intelligence_layer import TemporalIntelligenceLayer

# ============================================================================
# Singleton Instances
# ============================================================================

# Temporal Intelligence Layer: stateless, reusable across all operations
_temporal_intelligence_layer = TemporalIntelligenceLayer()


# ============================================================================
# Accessor Functions
# ============================================================================


def get_til() -> TemporalIntelligenceLayer:
    """
    Get the shared Temporal Intelligence Layer instance.

    Returns:
        TemporalIntelligenceLayer: The global singleton TIL instance
                                   (stateless, reusable)

    DESIGN NOTE:
        This instance is created once at application startup and reused
        for every request. Since TIL is stateless and deterministic,
        sharing a single instance is safe and efficient.

    Example:
        >>> til = get_til()
        >>> duration = til.estimate_duration("email_received", {})
        >>> slot = til.suggest_time_slot("user-1", "household-1", duration)
    """
    return _temporal_intelligence_layer
