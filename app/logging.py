"""Structured application logs with explicit privacy allowlists."""

import json
import logging
from datetime import UTC, datetime

from app.errors import ValidationReason
from app.startup import safe_diagnostic_fields

SERVICE_NAME = "review-backend"
SAFE_ERROR_CODES = frozenset(
    {
        "none",
        "authentication_required",
        "authentication_unavailable",
        "csrf_rejected",
        "empty_input",
        "empty_model_response",
        "history_unavailable",
        "idempotency_conflict",
        "initializing",
        "inference_connecting",
        "inference_draining",
        "inference_failed",
        "inference_stuck",
        "inference_timeout",
        "inference_unavailable",
        "input_too_large",
        "internal_error",
        "invalid_credentials",
        "invalid_cursor",
        "invalid_model_response",
        "login_unavailable",
        "method_not_allowed",
        "ollama_unavailable",
        "ollama_model_missing",
        "ollama_model_mismatch",
        "not_found",
        "queue_full",
        "rate_limited",
        "request_rejected",
        "review_active",
        "review_not_found",
        "session_expired",
        "startup_or_storage_failure",
        "storage_unavailable",
        "token_limit",
        "unsupported_media_type",
        "validation_error",
    }
)
SAFE_OUTCOMES = SAFE_ERROR_CODES | frozenset({"accepted", "completed", "rejected"})
SAFE_VALIDATION_REASONS = frozenset(reason.value for reason in ValidationReason)
SAFE_ROUTES = frozenset(
    {
        "/api/v1/auth/register",
        "/api/v1/auth/login",
        "/api/v1/auth/logout",
        "/api/v1/auth/me",
        "/api/v1/reviews",
        "/api/v1/reviews/{review_id}",
        "/api/v1/runtime",
        "/health/live",
        "/health/ready",
    }
)


class SafeFormatter(logging.Formatter):
    def __init__(self, *, environment="local", release_sha=None):
        super().__init__()
        self.environment = environment
        self.release_sha = release_sha

    def format(self, record):
        payload = {
            "timestamp": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": SERVICE_NAME,
            "environment": self.environment,
            "event": record.getMessage(),
        }
        if self.release_sha:
            payload["release_sha"] = self.release_sha
        for key in (
            "request_id",
            "status",
            "review_id",
            "method",
            "route",
            "queue_depth",
            "queue_wait_ms",
            "duration_ms",
            "generated_tokens",
            "output_limit_reached",
            "output_token_limit",
            "section_generated_tokens",
            "section_generation_ms",
            "section_first_token_ms",
            "section_input_tokens",
            "section_limits_reached",
            "section_trailing_fragments_removed",
        ):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if hasattr(record, "error_code"):
            code = record.error_code
            payload["error_code"] = code if code in SAFE_ERROR_CODES else "internal_error"
        if hasattr(record, "outcome"):
            outcome = record.outcome
            payload["outcome"] = outcome if outcome in SAFE_OUTCOMES else "inference_failed"
        if hasattr(record, "validation_reason"):
            reason = record.validation_reason
            if reason in SAFE_VALIDATION_REASONS:
                payload["validation_reason"] = reason
        payload.update(safe_diagnostic_fields(record.__dict__))
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(settings):
    handler = logging.StreamHandler()
    handler.setFormatter(
        SafeFormatter(environment=settings.environment, release_sha=settings.release_sha)
    )
    logger = logging.getLogger("review")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
