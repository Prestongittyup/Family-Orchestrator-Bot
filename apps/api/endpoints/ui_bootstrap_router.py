from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from apps.api.product_surface.chat_gateway_service import ChatGatewayService
from apps.api.product_surface.contracts import (
    ActionExecutionRequest,
    ChatMessageRequest,
    ChatResponse,
    UIBootstrapState,
)
from apps.api.product_surface.bootstrap_service import UIBootstrapService


router = APIRouter(prefix="/v1/ui", tags=["ui"])
_bootstrap_service = UIBootstrapService()
_chat_service = ChatGatewayService(bootstrap_service=_bootstrap_service)


@router.get("/bootstrap")
def get_ui_bootstrap(
    family_id: str = Query(..., description="Family scope (required)"),
) -> UIBootstrapState:
    try:
        return _bootstrap_service.get_state(family_id=family_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"bootstrap_failed: {exc}")


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
