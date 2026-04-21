from __future__ import annotations

from fastapi import APIRouter

from insights.contracts import InsightBridgeResponse
from insights.insight_engine import build_insight_response


router = APIRouter(prefix="/insights", tags=["insights"])


@router.get("/summary", response_model=InsightBridgeResponse)
def get_insight_summary() -> InsightBridgeResponse:
    return build_insight_response()


@router.get("/patterns", response_model=InsightBridgeResponse)
def get_insight_patterns() -> InsightBridgeResponse:
    return build_insight_response()


@router.get("/recommendations", response_model=InsightBridgeResponse)
def get_insight_recommendations() -> InsightBridgeResponse:
    return build_insight_response()