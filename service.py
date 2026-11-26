"""Compatibility entry point: ``from service import MeterService``."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from meter_service.core import (  # noqa: E402
    AuthorizationError,
    ConflictError,
    DEFAULT_SEED,
    MeterService,
    NotFoundError,
    ServiceError,
    ValidationError,
    load_seed,
)

__all__ = [
    "AuthorizationError",
    "ConflictError",
    "DEFAULT_SEED",
    "MeterService",
    "NotFoundError",
    "ServiceError",
    "ValidationError",
    "load_seed",
    "create_service",
    "build_report",
    "RentalService",
    "Service",
    "Engine",
]


def create_service(seed_path: str | None = None) -> MeterService:
    return MeterService(seed_path=seed_path)


def build_report(seed_path: str | None = None) -> dict:
    """Return the end-to-end local scenario result."""
    return MeterService(seed_path=seed_path).scenario_report()


# Common descriptive aliases for direct imports in local integrations.
RentalService = MeterService
Service = MeterService
Engine = MeterService
