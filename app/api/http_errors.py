"""Consistent HTTP errors without exposing input values or exception details."""

import logging
import sqlite3

from botocore.exceptions import BotoCoreError, ClientError
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.errors import AppError
from app.logging import SAFE_ERROR_CODES

logger = logging.getLogger("review")


def error_response(scope, code, message, status):
    state = scope.setdefault("state", {})
    state["error_code"] = code if code in SAFE_ERROR_CODES else "internal_error"
    return JSONResponse(
        status_code=status,
        content={"error": {"code": code, "message": message, "request_id": state["request_id"]}},
        headers={"Retry-After": "60"} if status == 429 else {},
    )


async def application_error(request, exc):
    return error_response(request.scope, exc.code, exc.message, exc.status)


async def validation_error(request, exc):
    # Pydantic errors include input values; never return those to clients or logs.
    return error_response(
        request.scope,
        "validation_error",
        "Invalid input. Check the identifier, password, code, or review ID.",
        422,
    )


async def storage_error(request, exc):
    return error_response(
        request.scope,
        "history_unavailable",
        "Review history could not be loaded or saved. Please retry.",
        503,
    )


async def users_error(request, exc):
    return error_response(
        request.scope,
        "authentication_unavailable",
        "The account service is unavailable. Please retry.",
        503,
    )


async def unexpected_error(request, exc):
    logger.error(
        "request_failed",
        extra={"request_id": request.state.request_id, "error_code": "internal_error"},
    )
    return error_response(
        request.scope, "internal_error", "The request could not be completed.", 500
    )


def register_error_handlers(app):
    for error_type, handler in (
        (AppError, application_error),
        (RequestValidationError, validation_error),
        (sqlite3.Error, storage_error),
        (BotoCoreError, users_error),
        (ClientError, users_error),
        (Exception, unexpected_error),
    ):
        app.add_exception_handler(error_type, handler)
