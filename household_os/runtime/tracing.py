from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any


def trace_function(**_trace_metadata: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """No-op runtime trace decorator used to avoid archive observability coupling."""

    def _decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        if getattr(func, "__code__", None) and func.__code__.co_flags & 0x80:
            @wraps(func)
            async def _async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

            return _async_wrapper

        @wraps(func)
        def _wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return _wrapper

    return _decorate
