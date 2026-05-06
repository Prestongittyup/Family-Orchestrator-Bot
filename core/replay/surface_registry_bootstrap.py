from __future__ import annotations


def ensure_surface_registry_initialized() -> None:
    # Sprint 21: registry is descriptor-driven and lazily resolved.
    # Keep this function as a backwards-compatible no-op for legacy imports.
    return


__all__ = ["ensure_surface_registry_initialized"]
