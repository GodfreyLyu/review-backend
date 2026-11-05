"""API readiness observes only its database and schema, independently of workers."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Readiness:
    status: str
    error_code: str | None = None


def check_readiness(store) -> Readiness:
    try:
        store.ping()
    except Exception:
        return Readiness(status="storage_unavailable", error_code="storage_unavailable")
    return Readiness(status="ready")
