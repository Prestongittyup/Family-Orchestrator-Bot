# ARCHIVE MODULE - NOT PART OF ACTIVE RUNTIME
# DO NOT IMPORT INTO app/

"""Deprecated archive system router.

Legacy system contracts are intentionally retired to enforce a single
runtime definition in app.main:app.
"""

from __future__ import annotations

from fastapi import APIRouter


router = APIRouter(prefix="/_archive/system-deprecated", tags=["system-deprecated"])

