# ARCHIVE MODULE - NOT PART OF ACTIVE RUNTIME
# DO NOT IMPORT INTO app/

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from archive.apps.api.endpoints.integrations_router import get_credential_store, get_http_client
from archive.apps.api.integration_core.credentials import InMemoryOAuthCredentialStore
from archive.apps.api.product_surface.chat_gateway_service import ChatGatewayService
from archive.apps.api.product_surface.contracts import (
    ActionExecutionRequest,
    ChatMessageRequest,
    ChatResponse,
    UIBootstrapState,
)
from archive.apps.api.product_surface.bootstrap_service import UIBootstrapService


router = APIRouter(prefix="/v1/ui", tags=["ui"])
_bootstrap_service = UIBootstrapService()
_chat_service = ChatGatewayService(bootstrap_service=_bootstrap_service)


@router.get("/bootstrap")
def get_ui_bootstrap(
    family_id: str = Query(..., description="Family scope (required)"),
    user_id: str | None = Query(None, description="Optional user scope for integration bootstrap fallback"),
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> UIBootstrapState:
    try:
        return _bootstrap_service.get_state(
            family_id=family_id,
            user_id=user_id,
            credential_store=credential_store,
            http_client=http_client,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"bootstrap_failed: {exc}")


@router.get("/email/detail")
def get_ui_email_detail(
    family_id: str = Query(..., description="Family scope (required)"),
    email_id: str = Query(..., description="Email identifier from notification or ingestion payload"),
) -> dict[str, Any]:
    try:
        return _bootstrap_service.get_email_detail(
            family_id=family_id,
            email_id=email_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except LookupError:
        raise HTTPException(status_code=404, detail="email_not_found")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"email_detail_failed: {exc}")


@router.post("/message")
def post_ui_message(request: ChatMessageRequest) -> ChatResponse:
    try:
        return _chat_service.process_message(
            family_id=request.family_id,
            message=request.message,
            session_id=request.session_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"message_gateway_failed: {exc}")


@router.post("/action")
def post_ui_action(request: ActionExecutionRequest) -> ChatResponse:
    try:
        return _chat_service.execute_action(
            family_id=request.family_id,
            session_id=request.session_id,
            action_card_id=request.action_card_id,
            payload=request.payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"action_gateway_failed: {exc}")

