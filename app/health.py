"""Shared readiness observation, independent of HTTP response handling."""

import sqlite3
from dataclasses import dataclass

from app.coordinator import Coordinator
from app.logging import SAFE_ERROR_CODES
from app.persistence.storage import Store


@dataclass(frozen=True)
class Readiness:
    status: str
    error_code: str | None = None


def check_readiness(coordinator: Coordinator, store: Store) -> Readiness:
    if not coordinator.ready:
        return Readiness(
            status=coordinator.state,
            error_code=(
                coordinator.state if coordinator.state in SAFE_ERROR_CODES else "request_rejected"
            ),
        )
    try:
        # Keep the existing writable-store check, including expired-session cleanup.
        # Loading/draining/closing must not reach storage through a readiness probe.
        store.ping()
    except sqlite3.Error:
        return Readiness(status="storage_unavailable", error_code="storage_unavailable")
    return Readiness(status="ready")
