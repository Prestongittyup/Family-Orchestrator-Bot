# ARCHIVE MODULE - NOT PART OF ACTIVE RUNTIME
# DO NOT IMPORT INTO app/

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from archive.apps.api.ingestion.adapters.provider_email_adapter import ProviderEmailAdapter
from archive.apps.api.ingestion.models import EmailInput, IngestionError, WebhookPayload
from archive.apps.api.ingestion.service import ingest_email, ingest_webhook


router = APIRouter(prefix="/ingest", tags=["ingestion"])


class EmailIngestRequest(EmailInput):
    household_id: str | None = Field(default=None, description="Optional household override for event routing")


class ProviderEmailPayloadRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict, description="Provider-native email payload")
    household_id: str | None = Field(default=None, description="Optional household override for event routing")


class ProviderEmailBatchRequest(BaseModel):
    payloads: list[dict[str, Any]] = Field(default_factory=list, description="Provider-native email payloads")
    household_id: str | None = Field(default=None, description="Optional household override for event routing")


def _ingest_provider_payload(
    *,
    adapter: ProviderEmailAdapter,
    payload: dict[str, Any],
    household_id: str | None,
) -> dict[str, Any]:
    parsed = adapter.parse_message(payload)
    return ingest_email(
        email_id=parsed.email_id,
        sender=parsed.sender,
        recipient=parsed.recipient,
        subject=parsed.subject,
        body=parsed.body,
        received_at=parsed.received_at,
        provider=parsed.provider,
        household_id=household_id,
        thread_id=parsed.thread_id,
        latest_message_id=parsed.latest_message_id,
        thread_messages=list(parsed.thread_messages or []),
        to_me=parsed.to_me,
        cc_me=parsed.cc_me,
    )


@router.post("/webhook")
def post_ingest_webhook(payload: WebhookPayload) -> dict[str, Any]:
    try:
        return ingest_webhook(payload.model_dump())
    except IngestionError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"message": exc.message, "detail": exc.detail},
        )


@router.post("/email")
def post_ingest_email(payload: EmailIngestRequest) -> dict[str, Any]:
    try:
        return ingest_email(
            email_id=payload.email_id,
            sender=payload.sender,
            recipient=payload.recipient,
            subject=payload.subject,
            body=payload.body,
            received_at=payload.received_at.isoformat(),
            provider=payload.provider,
            household_id=payload.household_id,
            thread_id=payload.thread_id,
            latest_message_id=payload.latest_message_id,
            thread_messages=list(payload.thread_messages or []),
            to_me=payload.to_me,
            cc_me=payload.cc_me,
        )
    except IngestionError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"message": exc.message, "detail": exc.detail},
        )


@router.post("/email/provider/{provider}")
def post_ingest_provider_email(
    provider: str,
    payload: ProviderEmailPayloadRequest,
) -> dict[str, Any]:
    adapter = ProviderEmailAdapter(provider_name=provider)
    try:
        return _ingest_provider_payload(
            adapter=adapter,
            payload=payload.payload,
            household_id=payload.household_id,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Provider payload validation failed",
                "detail": {"error": str(exc)},
            },
        )
    except IngestionError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"message": exc.message, "detail": exc.detail},
        )


@router.post("/email/provider/{provider}/batch")
def post_ingest_provider_email_batch(
    provider: str,
    payload: ProviderEmailBatchRequest,
) -> dict[str, Any]:
    adapter = ProviderEmailAdapter(provider_name=provider)
    results: list[dict[str, Any]] = []
    processed_count = 0
    failed_count = 0

    for index, raw_payload in enumerate(payload.payloads):
        try:
            result = _ingest_provider_payload(
                adapter=adapter,
                payload=raw_payload,
                household_id=payload.household_id,
            )
            processed_count += 1
            results.append(
                {
                    "index": index,
                    "status": "processed",
                    "result": result,
                }
            )
        except (ValueError, IngestionError) as exc:
            failed_count += 1
            if isinstance(exc, IngestionError):
                error_payload = {
                    "message": exc.message,
                    "detail": exc.detail,
                    "status_code": exc.status_code,
                }
            else:
                error_payload = {
                    "message": "Provider payload validation failed",
                    "detail": {"error": str(exc)},
                    "status_code": 422,
                }

            results.append(
                {
                    "index": index,
                    "status": "failed",
                    "error": error_payload,
                }
            )

    return {
        "status": "ok",
        "provider": adapter.provider_name,
        "count": len(results),
        "processed_count": processed_count,
        "failed_count": failed_count,
        "results": results,
    }
