"""Conventional application entry point.

``python3 app.py`` starts the JSON service.  Tests and local integrations can
import :data:`service` or call :func:`create_server`.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from meter_service import MeterService  # noqa: E402
from meter_service.server import build_server, main  # noqa: E402

service = MeterService()


def create_server(host: str = "127.0.0.1", port: int = 8000) -> object:
    return build_server(service, host=host, port=port)


app = service
application = service


if __name__ == "__main__":
    raise SystemExit(main())
