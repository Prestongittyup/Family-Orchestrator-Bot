from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol


class GeminiProviderError(RuntimeError):
    pass


class GeminiStreamTransport(Protocol):
    def __call__(
        self,
        *,
        api_key: str,
        model: str,
        temperature: float,
        prompt: str,
        max_output_tokens: int,
        response_mime_type: str,
        timeout_seconds: float,
    ) -> AsyncIterator[str]: ...


class GeminiProvider:
    name = "gemini"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gemini-1.5-flash",
        temperature: float = 0.2,
        timeout_seconds: float = 45.0,
        stream_transport: GeminiStreamTransport | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._temperature = temperature
        self._timeout_seconds = timeout_seconds
        self._stream_transport = stream_transport

    @property
    def has_api_key(self) -> bool:
        return bool(self._api_key.strip())

    async def stream_json_text(
        self,
        *,
        prompt: str,
        max_output_tokens: int = 768,
        response_mime_type: str = "application/json",
    ) -> AsyncIterator[str]:
        if self._stream_transport is None:
            raise GeminiProviderError("gemini_transport_not_configured")

        async for chunk in self._stream_transport(
            api_key=self._api_key,
            model=self._model,
            temperature=self._temperature,
            prompt=prompt,
            max_output_tokens=max_output_tokens,
            response_mime_type=response_mime_type,
            timeout_seconds=self._timeout_seconds,
        ):
            if chunk:
                yield chunk
