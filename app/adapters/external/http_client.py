from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx


class ExternalHTTPError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class ExternalHTTPTransientError(ExternalHTTPError):
    pass


class ExternalHTTPPermanentError(ExternalHTTPError):
    pass


class ExternalHTTPClient:
    def __init__(self, *, timeout_seconds: float = 45.0, connect_timeout_seconds: float = 10.0) -> None:
        self._timeout_seconds = timeout_seconds
        self._connect_timeout_seconds = connect_timeout_seconds

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self._timeout_seconds, connect=self._connect_timeout_seconds)

    async def stream_lines(
        self,
        *,
        method: str,
        url: str,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[str]:
        try:
            async with httpx.AsyncClient(timeout=self._timeout()) as client:
                async with client.stream(method.upper(), url, json=json_body, headers=headers) as response:
                    if response.status_code >= 400:
                        error_body = (await response.aread()).decode("utf-8", errors="ignore")
                        message = f"HTTP request failed [{response.status_code}]: {error_body}"
                        if response.status_code in {408, 429, 500, 502, 503, 504}:
                            raise ExternalHTTPTransientError(message, status_code=response.status_code)
                        raise ExternalHTTPPermanentError(message, status_code=response.status_code)

                    async for line in response.aiter_lines():
                        yield line
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise ExternalHTTPTransientError(str(exc)) from exc
