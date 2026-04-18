"""
Central ingestion service router.

Ingests external events (webhooks, emails) and routes them through the
existing OS-1 /event pipeline without modifying core logic.

All external inputs are normalized to OS-1 format, then passed to the
same route_event() function used by the HTTP /event endpoint.
"""
from __future__ import annotations

from typing import Any

from apps.api.core.feature_flags import resolve_feature_flags
from apps.api.ingestion.failure_handling import build_failure_trace, quarantine_ingestion_payload
from apps.api.ingestion.models import (
    EmailValidationError,
    IngestionError,
    WebhookValidationError,
    validate_email_payload,
    validate_webhook_payload,
)
from apps.api.ingestion.normalization import convert_webhook_to_os1_event, convert_email_to_os1_event
from apps.api.observability.trace_context import ensure_event_payload_trace
from apps.api.services.router_service import route_event
from apps.api.schemas.event import SystemEvent


def _debug_payload(enabled: bool, **data: Any) -> dict[str, Any] | None:
    if not enabled:
        return None
    return data


def ingest_webhook(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Ingest external webhook payload.
    
    Validates, normalizes, and routes through OS-1 pipeline.
    
    Args:
        payload: Raw webhook payload
        
    Returns:
        {
            "status": "success" | "error",
            "event_id": str (if success),
            "message": str (if error)
        }
        
    Raises:
        IngestionError: If validation or routing fails
    """
    webhook = None
    os1_event = None
    try:
        # 1) Validation at ingestion boundary
        webhook = validate_webhook_payload(payload)

        # 2) Normalize to OS-1 event format
        os1_event = convert_webhook_to_os1_event(webhook)
        flags = resolve_feature_flags(household_id=str(os1_event["household_id"]))
        if not flags.ingestion_enabled:
            response: dict[str, Any] = {
                "status": "disabled",
                "event_id": os1_event["idempotency_key"],
                "message": "Webhook ingestion is disabled by runtime feature flag",
            }
            debug = _debug_payload(
                flags.debug_mode,
                household_id=os1_event["household_id"],
                feature_flags={
                    "ingestion_enabled": flags.ingestion_enabled,
                    "tracing_enabled": flags.tracing_enabled,
                    "debug_mode": flags.debug_mode,
                },
            )
            if debug is not None:
                response["debug"] = debug
            return response

        trace_id: str | None = None
        if flags.tracing_enabled:
            trace_id = ensure_event_payload_trace(
                os1_event.setdefault("payload", {}),
                idempotency_key=str(os1_event["idempotency_key"]),
                source=str(os1_event["source"]),
                event_type=str(os1_event["type"]),
                stage="os1_ingestion",
            )

        # 3) Convert dict to SystemEvent and route through existing path
        event = SystemEvent(**os1_event)
        result = route_event(event)

        if isinstance(result, dict) and result.get("status") == "queue_full":
            raise IngestionError(
                message="Webhook ingestion queue is full",
                detail={
                    "status": "queue_full",
                    "queue_size": result.get("queue_size"),
                },
                status_code=503,
            )

        if isinstance(result, dict) and result.get("status") == "duplicate_ignored":
            response = {
                "status": "duplicate_ignored",
                "event_id": os1_event["idempotency_key"],
                "idempotency_key": result.get("idempotency_key"),
                "message": "Webhook duplicate ignored by idempotency guard",
            }
            if trace_id is not None:
                response["trace_id"] = trace_id
            debug = _debug_payload(
                flags.debug_mode,
                household_id=os1_event["household_id"],
                feature_flags={
                    "ingestion_enabled": flags.ingestion_enabled,
                    "tracing_enabled": flags.tracing_enabled,
                    "debug_mode": flags.debug_mode,
                },
            )
            if debug is not None:
                response["debug"] = debug
            return response

        response = {
            "status": "success",
            "event_id": os1_event["idempotency_key"],
            "message": f"Webhook ingested successfully (source={webhook.source}, type={webhook.type})",
        }
        if trace_id is not None:
            response["trace_id"] = trace_id
        debug = _debug_payload(
            flags.debug_mode,
            household_id=os1_event["household_id"],
            feature_flags={
                "ingestion_enabled": flags.ingestion_enabled,
                "tracing_enabled": flags.tracing_enabled,
                "debug_mode": flags.debug_mode,
            },
        )
        if debug is not None:
            response["debug"] = debug
        return response
    except IngestionError:
        raise
    except Exception as e:
        source = webhook.source if webhook is not None else payload.get("source")
        event_type = webhook.type if webhook is not None else payload.get("type")
        stage = "validation" if isinstance(e, WebhookValidationError) else "normalization_or_routing"
        trace = build_failure_trace(
            adapter="webhook",
            stage=stage,
            error_type=type(e).__name__,
            error_message=str(e),
            source=source,
            event_type=event_type,
        )
        quarantine_ref = quarantine_ingestion_payload(adapter="webhook", payload=payload, trace=trace)
        raise IngestionError(
            message="Webhook ingestion failed",
            detail={
                "status": "quarantined",
                "failure_trace": trace,
                "quarantine": quarantine_ref,
            },
            status_code=400,
        )


def ingest_email(
    email_id: str,
    sender: str,
    recipient: str,
    subject: str,
    body: str,
    received_at: str,  # ISO-8601 timestamp
    provider: str = "generic"
) -> dict[str, Any]:
    """
    Ingest email message.
    
    Converts email to OS-1 event and routes through pipeline.
    
    Args:
        email_id: Unique email identifier from provider
        sender: Email sender address
        recipient: Email recipient address
        subject: Email subject line
        body: Email body text
        received_at: ISO-8601 timestamp of receipt
        provider: Email provider ('gmail', 'outlook', 'imap', 'generic')
        
    Returns:
        {
            "status": "success" | "error",
            "event_id": str (if success),
            "message": str (if error)
        }
        
    Raises:
        IngestionError: If conversion or routing fails
    """
    payload = {
        "email_id": email_id,
        "sender": sender,
        "recipient": recipient,
        "subject": subject,
        "body": body,
        "received_at": received_at,
        "provider": provider,
    }

    email = None
    os1_event = None
    try:
        # 1) Validate email payload at ingestion boundary
        email = validate_email_payload(payload)

        # 2) Convert to OS-1 event format
        os1_event = convert_email_to_os1_event(
            email_id=email.email_id,
            sender=email.sender,
            subject=email.subject,
            body=email.body,
            received_at=email.received_at,
            provider=email.provider,
        )
        flags = resolve_feature_flags(household_id=str(os1_event["household_id"]))
        if not flags.ingestion_enabled:
            response: dict[str, Any] = {
                "status": "disabled",
                "event_id": os1_event["idempotency_key"],
                "message": "Email ingestion is disabled by runtime feature flag",
            }
            debug = _debug_payload(
                flags.debug_mode,
                household_id=os1_event["household_id"],
                feature_flags={
                    "ingestion_enabled": flags.ingestion_enabled,
                    "tracing_enabled": flags.tracing_enabled,
                    "debug_mode": flags.debug_mode,
                },
            )
            if debug is not None:
                response["debug"] = debug
            return response

        trace_id: str | None = None
        if flags.tracing_enabled:
            trace_id = ensure_event_payload_trace(
                os1_event.setdefault("payload", {}),
                idempotency_key=str(os1_event["idempotency_key"]),
                source=str(os1_event["source"]),
                event_type=str(os1_event["type"]),
                stage="os1_ingestion",
            )

        # 3) Convert dict to SystemEvent and route through existing path
        event = SystemEvent(**os1_event)
        result = route_event(event)

        if isinstance(result, dict) and result.get("status") == "queue_full":
            raise IngestionError(
                message="Email ingestion queue is full",
                detail={
                    "status": "queue_full",
                    "queue_size": result.get("queue_size"),
                },
                status_code=503,
            )

        if isinstance(result, dict) and result.get("status") == "duplicate_ignored":
            response = {
                "status": "duplicate_ignored",
                "event_id": os1_event["idempotency_key"],
                "idempotency_key": result.get("idempotency_key"),
                "message": "Email duplicate ignored by idempotency guard",
            }
            if trace_id is not None:
                response["trace_id"] = trace_id
            debug = _debug_payload(
                flags.debug_mode,
                household_id=os1_event["household_id"],
                feature_flags={
                    "ingestion_enabled": flags.ingestion_enabled,
                    "tracing_enabled": flags.tracing_enabled,
                    "debug_mode": flags.debug_mode,
                },
            )
            if debug is not None:
                response["debug"] = debug
            return response

        response = {
            "status": "success",
            "event_id": os1_event["idempotency_key"],
            "message": f"Email ingested successfully (from={email.sender}, subject_len={len(email.subject)})",
        }
        if trace_id is not None:
            response["trace_id"] = trace_id
        debug = _debug_payload(
            flags.debug_mode,
            household_id=os1_event["household_id"],
            feature_flags={
                "ingestion_enabled": flags.ingestion_enabled,
                "tracing_enabled": flags.tracing_enabled,
                "debug_mode": flags.debug_mode,
            },
        )
        if debug is not None:
            response["debug"] = debug
        return response
    except IngestionError:
        raise
    except Exception as e:
        stage = "validation" if isinstance(e, EmailValidationError) else "normalization_or_routing"
        trace = build_failure_trace(
            adapter="email",
            stage=stage,
            error_type=type(e).__name__,
            error_message=str(e),
            source="email_ingestion",
            event_type="email_received",
        )
        quarantine_ref = quarantine_ingestion_payload(adapter="email", payload=payload, trace=trace)
        raise IngestionError(
            message="Email ingestion failed",
            detail={
                "status": "quarantined",
                "failure_trace": trace,
                "quarantine": quarantine_ref,
            },
            status_code=400,
        )
