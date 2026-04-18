from __future__ import annotations

import logging
import os
from threading import Lock

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


logger = logging.getLogger(__name__)

_BOOTSTRAP_LOCK = Lock()
_BOOTSTRAPPED = False
_REQUIRED_GOOGLE_ENV = (
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REDIRECT_URI",
)


def load_environment(*, force: bool = False) -> None:
    """
    Idempotently load environment variables from .env.

    Safe to call multiple times. Use force=True in tests when the working
    directory and .env fixture changes between calls.
    """
    global _BOOTSTRAPPED

    with _BOOTSTRAP_LOCK:
        if _BOOTSTRAPPED and not force:
            return

        if load_dotenv is None:
            logger.warning("python-dotenv is not installed; .env file will not be auto-loaded.")
        else:
            # Load only the .env in the current working directory to keep
            # startup and tests deterministic.
            load_dotenv(dotenv_path=".env", override=force)

        _BOOTSTRAPPED = True


def get_missing_google_oauth_env_vars() -> list[str]:
    """Return missing Google OAuth env var names (empty list if fully configured)."""
    return [key for key in _REQUIRED_GOOGLE_ENV if not str(os.environ.get(key, "")).strip()]
