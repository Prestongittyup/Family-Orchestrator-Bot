from __future__ import annotations

import sys

from archive.apps.api.integration_core import os1_bridge as _archive_os1_bridge


sys.modules[__name__] = _archive_os1_bridge
