"""Bounded startup diagnostics: never format exceptions, URLs or model/user content."""

import re

from app.errors import AppError

STAGES = frozenset(
    {
        "storage_initialize",
        "users_storage",
        "queue_recovery",
        "inference_handshake",
        "storage_recheck",
        "queue_processing",
    }
)


def safe_diagnostic_fields(fields):
    result = {}
    if fields.get("stage") in STAGES:
        result["stage"] = fields["stage"]
    name = fields.get("exception_type")
    if isinstance(name, str) and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", name):
        result["exception_type"] = name
    for key, lower, upper in (
        ("errno", 1, 4095),
        ("startup_attempt", 0, 1000),
        ("startup_wait_ms", 0, 10000),
        ("startup_elapsed_ms", 0, 3600000),
    ):
        value = fields.get(key)
        if type(value) is int and lower <= value <= upper:
            result[key] = value
    return result


def failure_fields(exc):
    fields = safe_diagnostic_fields(
        {
            "exception_type": type(exc).__name__,
            "errno": getattr(exc, "errno", None),
        }
    )
    fields["error_code"] = (
        exc.code
        if isinstance(exc, AppError)
        and exc.code
        in {
            "inference_unavailable",
            "ollama_unavailable",
            "ollama_model_missing",
            "ollama_model_mismatch",
            "inference_timeout",
            "invalid_model_response",
        }
        else "startup_or_storage_failure"
    )
    return fields
