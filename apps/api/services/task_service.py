from __future__ import annotations

import sys

from archive.apps.api.services import task_service as _archive_task_service


sys.modules[__name__] = _archive_task_service
from archive.apps.api.services.canonical_event_router import canonical_event_router

