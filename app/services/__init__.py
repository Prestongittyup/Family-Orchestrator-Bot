"""Service layer contract.

Layer responsibility:
- orchestrate domain workflows and coordinate adapters/gateways

Allowed internal imports:
- app.services.*
- app.schemas.*
- app.adapters.* (through public interfaces)

Forbidden internal imports:
- app.api.*
- direct external SDK/library calls (httpx, redis.asyncio, sqlalchemy)
"""

from core.architecture.architecture_guard import enforce_import_boundary

enforce_import_boundary(module_path=__name__, module_file=__file__)

