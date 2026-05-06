from __future__ import annotations

import sys

from archive.apps.api.observability import metrics as _archive_metrics


sys.modules[__name__] = _archive_metrics
