"""Adapter layer contract.

Layer responsibility:
- own infrastructure integration and external system interaction

Allowed internal imports:
- app.adapters.*

Forbidden internal imports:
- app.services.*
- app.api.*
"""

from app.adapters.cache.redis_client import RedisCacheClient
from app.adapters.db.client import DatabaseClient
from app.adapters.llm.gateway import LLMGateway

__all__ = ["RedisCacheClient", "DatabaseClient", "LLMGateway"]
