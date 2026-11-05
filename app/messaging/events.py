"""Backend-owned reference-only review.jobs.v1 wire contract."""

import json
import math
from dataclasses import dataclass
from uuid import UUID

EXECUTION_VERSION = "review-v1"
MAX_EVENT_BYTES = 16384


class InvalidEvent(ValueError):
    """Safe machine-readable reason; never includes message content."""


@dataclass(frozen=True)
class ReviewJob:
    event_id: str
    review_id: str
    attempt: int
    trace_id: str
    created_at: float
    schema_version: int = 1
    execution_version: str = EXECUTION_VERSION


def decode_job(payload: bytes, key: bytes | None) -> ReviewJob:
    if not isinstance(payload, bytes) or len(payload) > MAX_EVENT_BYTES:
        raise InvalidEvent("invalid_size")
    try:
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError
        for field in ("event_id", "review_id", "trace_id"):
            if not isinstance(data[field], str) or str(UUID(data[field])) != data[field]:
                raise ValueError
        if type(data["schema_version"]) is not int or data["schema_version"] != 1:
            raise InvalidEvent("unsupported_schema")
        if data["execution_version"] != EXECUTION_VERSION:
            raise InvalidEvent("unsupported_execution")
        if type(data["attempt"]) is not int or not 1 <= data["attempt"] <= 1000:
            raise ValueError
        created = data["created_at"]
        if type(created) not in (float, int) or not math.isfinite(created) or created <= 0:
            raise ValueError
        if key != data["review_id"].encode():
            raise InvalidEvent("invalid_key")
        if set(data) - {
            "event_id",
            "review_id",
            "attempt",
            "trace_id",
            "created_at",
            "schema_version",
            "execution_version",
            "reason",
        }:
            raise InvalidEvent("unexpected_fields")
        if "reason" in data and (not isinstance(data["reason"], str) or len(data["reason"]) > 100):
            raise ValueError
        return ReviewJob(**{k: v for k, v in data.items() if k != "reason"})
    except InvalidEvent:
        raise
    except (ValueError, KeyError, TypeError, UnicodeError):
        raise InvalidEvent("invalid_envelope") from None
