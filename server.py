"""Compatibility entry point: ``python3 server.py --demo``."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from meter_service.server import build_server, main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
