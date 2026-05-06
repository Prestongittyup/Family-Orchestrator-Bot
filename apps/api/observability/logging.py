from __future__ import annotations

import sys

from archive.apps.api.observability import logging as _archive_logging


sys.modules[__name__] = _archive_logging
