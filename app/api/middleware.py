"""Bound HTTP bodies before parsing and keep transport errors and logs private."""

import logging
from time import monotonic
from uuid import uuid4

from app.api.http_errors import error_response
from app.errors import AppError
from app.logging import SAFE_ERROR_CODES, SAFE_ROUTES
from app.monitoring import Metrics

logger = logging.getLogger("review")
BODY_METHODS = frozenset({"POST", "PUT", "PATCH"})
ALLOWED_METHODS = BODY_METHODS | {"GET", "HEAD", "DELETE", "OPTIONS"}
MAX_BODY_BYTES = 131072


def log_request(scope, status, duration):
    route = getattr(scope.get("route"), "path", None)
    route = route if route in SAFE_ROUTES else None
    if route in {"/health/live", "/health/ready"} and status < 400:
        return
    code = scope["state"].get("error_code")
    if code not in SAFE_ERROR_CODES:
        code = {404: "not_found", 405: "method_not_allowed"}.get(
            status, "request_rejected" if status >= 400 else "none"
        )
    fields = {
        "request_id": scope["state"]["request_id"],
        "status": status,
        "method": scope["method"] if scope["method"] in ALLOWED_METHODS else "OTHER",
        "duration_ms": max(0, round(duration * 1000)),
        "error_code": code,
    }
    if route is not None:
        fields["route"] = route
    logger.info("http_request", extra=fields)


async def read_body(scope, receive):
    """Return bounded JSON bytes, or None when the client disconnects."""
    headers = dict(scope["headers"])
    if headers.get(b"content-type", b"").split(b";")[0].strip() != b"application/json":
        raise AppError("unsupported_media_type", "Use application/json.", 415)
    body = bytearray()
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return None
        body.extend(message.get("body", b""))
        if len(body) > MAX_BODY_BYTES:
            raise AppError("input_too_large", "The request is too large.", 413)
        if not message.get("more_body", False):
            return bytes(body)


class RequestGuard:
    def __init__(self, app, metrics: Metrics | None = None):
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request_id = str(uuid4())
        scope.setdefault("state", {})["request_id"] = request_id
        started_at = monotonic()
        started = False

        async def safe_send(message):
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                message["headers"] += [
                    (b"x-request-id", request_id.encode()),
                    (b"cache-control", b"no-store"),
                    (b"x-content-type-options", b"nosniff"),
                ]
                # The internal scrape is not user traffic and must not observe itself.
                if scope.get("path") != "/metrics":
                    duration = max(0, monotonic() - started_at)
                    log_request(scope, message["status"], duration)
                    if self.metrics is not None:
                        self.metrics.http_response(
                            scope["method"],
                            getattr(scope.get("route"), "path", None),
                            message["status"],
                            duration,
                        )
            await send(message)

        try:
            replay = await self.prepare_receive(scope, receive)
        except AppError as exc:
            response = error_response(scope, exc.code, exc.message, exc.status)
            return await response(scope, receive, safe_send)
        if replay is None:
            return
        try:
            await self.app(scope, replay, safe_send)
        except Exception:
            # Never let transport logging expose exceptions containing user input.
            logger.error(
                "request_failed",
                extra={"request_id": request_id, "error_code": "internal_error"},
            )
            if not started:
                response = error_response(
                    scope, "internal_error", "The request could not be completed.", 500
                )
                await response(scope, replay, safe_send)
            else:
                await send({"type": "http.response.body", "body": b"", "more_body": False})

    @staticmethod
    async def prepare_receive(scope, receive):
        if scope["method"] not in BODY_METHODS:
            return receive
        body = await read_body(scope, receive)
        if body is None:
            return None
        consumed = False

        async def replay():
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        return replay
