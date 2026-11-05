import hashlib
import hmac
import secrets
import time
from typing import Annotated

from argon2 import PasswordHasher, Type
from argon2.exceptions import VerificationError
from fastapi import Depends, Request, Response

from app.api.schemas import SessionResponse
from app.config import Settings
from app.domain import SessionRecord, UserRecord
from app.errors import AppError

PASSWORDS = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1, type=Type.ID)
DUMMY_HASH = PASSWORDS.hash(secrets.token_urlsafe(32))


def check_origin(request: Request) -> None:
    if request.headers.get("origin") != request.app.state.settings.allowed_origin:
        raise AppError("csrf_rejected", "The request origin is not allowed.", 403)


def cookie_name(settings: Settings) -> str:
    return "__Host-review_session" if settings.cookie_secure else "review_session"


def signature(value: str, settings: Settings) -> str:
    return hmac.new(
        settings.signing_secret.get_secret_value().encode(), value.encode(), hashlib.sha256
    ).hexdigest()


def require_session(request: Request) -> SessionRecord:
    settings = request.app.state.settings
    token = request.cookies.get(cookie_name(settings), "")
    raw, _, signed = token.partition(".")
    if not raw or not hmac.compare_digest(signed.encode(), signature(raw, settings).encode()):
        raise AppError("authentication_required", "Please sign in to continue.", 401)
    session = request.app.state.store.session_get(token)
    if not session:
        raise AppError("session_expired", "Your session has expired. Please sign in again.", 401)
    user = request.app.state.users.get(session["login_id"])
    if not user or user.get("disabled") or user["user_id"] != session["user_id"]:
        raise AppError("authentication_required", "Please sign in to continue.", 401)
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        check_origin(request)
        supplied = request.headers.get("x-csrf-token", "")
        if not hmac.compare_digest(supplied.encode(), session["csrf_token"].encode()):
            raise AppError(
                "csrf_rejected", "The security token is invalid. Refresh and retry.", 403
            )
    return session


def new_session(request: Request, response: Response, user: UserRecord) -> SessionResponse:
    settings = request.app.state.settings
    raw, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    token = raw + "." + signature(raw, settings)
    old = request.cookies.get(cookie_name(settings))
    if old:
        request.app.state.store.session_delete(old)
    expires = time.time() + settings.session_ttl_seconds
    request.app.state.store.session_create(token, user, csrf, expires)
    response.set_cookie(
        cookie_name(settings),
        token,
        max_age=settings.session_ttl_seconds,
        secure=settings.cookie_secure,
        httponly=True,
        samesite="strict",
        path="/",
    )
    return SessionResponse(
        login_id=user["login_id"],
        csrf_token=csrf,
        expires_at=expires,
        source_max_chars=settings.source_max_chars,
    )


def verify_password(user: UserRecord | None, password: str) -> UserRecord:
    try:
        valid = PASSWORDS.verify(user["password_hash"] if user else DUMMY_HASH, password)
    except VerificationError:
        valid = False
    if not valid or not user or user.get("disabled"):
        raise AppError("invalid_credentials", "Incorrect login identifier or password.", 401)

    return user


Session = Annotated[SessionRecord, Depends(require_session)]
