"""Rental meter qualification, handoff and measurement review service."""

from .core import (
    AuthorizationError,
    ConflictError,
    MeterService,
    NotFoundError,
    ServiceError,
    ValidationError,
    load_seed,
)

__all__ = [
    "AuthorizationError",
    "ConflictError",
    "MeterService",
    "NotFoundError",
    "ServiceError",
    "ValidationError",
    "build_server",
    "load_seed",
    "main",
]


def __getattr__(name):
    # Lazy import avoids a runpy double-import warning for
    # ``python -m meter_service.server``.
    if name in {"build_server", "main"}:
        from . import server as _server

        return getattr(_server, name)
    raise AttributeError(name)
