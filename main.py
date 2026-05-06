"""Deprecated root runtime shim.

Production backend entrypoint is app.main:app.
This module intentionally does not expose app objects.
"""

CANONICAL_ASGI_TARGET = "app.main:app"

__all__ = ["CANONICAL_ASGI_TARGET"]
