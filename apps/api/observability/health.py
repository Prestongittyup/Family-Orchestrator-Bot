"""Deprecated archive health router.

Legacy health/readiness endpoints are intentionally removed to avoid shadow
runtime contracts outside app.main:app.
"""
from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["observability-deprecated"])
