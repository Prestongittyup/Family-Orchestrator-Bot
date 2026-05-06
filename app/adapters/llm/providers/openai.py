from __future__ import annotations

from collections.abc import AsyncIterator


class OpenAIProviderError(RuntimeError):
    pass


class OpenAIProvider:
    """Stub provider kept for controlled fallback wiring.

    This adapter intentionally raises until OpenAI integration is implemented.
    """

    name = "openai"

    def __init__(self, *, api_key: str) -> None:
        self._api_key = api_key

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
        _ = (prompt, max_output_tokens, response_mime_type)
        raise OpenAIProviderError("OpenAI provider is not implemented")
