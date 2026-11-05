"""Account endpoints; session and CSRF rules live in app.api.auth."""

import hashlib

from fastapi import APIRouter, Request, Response

from app.api.auth import (
    PASSWORDS,
    Session,
    check_origin,
    cookie_name,
    new_session,
    verify_password,
)
from app.api.dependencies import Accounts, AppSettings, RequestLimiter, ReviewStore
from app.api.schemas import API_ERROR_RESPONSES, Credentials, SessionResponse
from app.config import Settings
from app.rate_limit import RateLimiter

router = APIRouter(prefix="/api/v1/auth", responses=API_ERROR_RESPONSES)


def auth_limit(request: Request, login_id: str, config: Settings, limiter: RateLimiter) -> None:
    # Only the direct peer is trusted; caller-supplied forwarding headers grant no new quota.
    client = request.client.host if request.client else "unknown"
    limiter.check("auth-ip:" + client, config.login_rate_limit)
    limiter.check(
        "auth-login:" + hashlib.sha256(login_id.encode()).hexdigest(), config.login_rate_limit
    )


@router.post("/register", status_code=201, response_model=SessionResponse)
def register(
    payload: Credentials,
    request: Request,
    response: Response,
    users: Accounts,
    config: AppSettings,
    limiter: RequestLimiter,
) -> SessionResponse:
    check_origin(request)
    auth_limit(request, payload.login_id, config, limiter)
    user = users.create(payload.login_id, PASSWORDS.hash(payload.password))
    return new_session(request, response, user)


@router.post("/login", response_model=SessionResponse)
def login(
    payload: Credentials,
    request: Request,
    response: Response,
    users: Accounts,
    config: AppSettings,
    limiter: RequestLimiter,
) -> SessionResponse:
    check_origin(request)
    auth_limit(request, payload.login_id, config, limiter)
    user = users.get(payload.login_id)
    user = verify_password(user, payload.password)
    return new_session(request, response, user)


@router.post("/logout", status_code=204)
def logout(
    request: Request, response: Response, session: Session, config: AppSettings, store: ReviewStore
) -> None:
    store.session_delete(request.cookies[cookie_name(config)])
    response.delete_cookie(
        cookie_name(config),
        path="/",
        secure=config.cookie_secure,
        httponly=True,
        samesite="strict",
    )


@router.get("/me", response_model=SessionResponse)
def me(session: Session, config: AppSettings) -> SessionResponse:
    return SessionResponse(
        login_id=session["login_id"],
        csrf_token=session["csrf_token"],
        expires_at=session["expires_at"],
        source_max_chars=config.source_max_chars,
    )
