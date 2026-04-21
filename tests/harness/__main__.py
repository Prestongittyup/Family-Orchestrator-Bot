"""
Harness Module Entry Point

Enables:
  python -m tests.harness [command] [options]
"""

import asyncio
import sys
from tests.harness.cli import main


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
