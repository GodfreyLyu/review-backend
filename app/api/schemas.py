"""Public HTTP contracts. Internal/storage-only fields are never serialized."""

import re
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from app.domain import ReviewStatus


class Credentials(BaseModel):
    login_id: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=12, max_length=128)

    @field_validator("login_id", mode="before")
    @classmethod
    def normalize(cls, value):
        if not isinstance(value, str):
            raise ValueError("Login identifier must be text.")
        value = value.strip().lower()
        if not re.fullmatch(r"[a-z0-9][a-z0-9._@+\-]{2,99}", value):
            raise ValueError("Use 3–100 letters, digits, dots, underscores, @, + or hyphens.")
        return value


class ReviewInput(BaseModel):
    source_code: str = Field(min_length=1, max_length=32000)
    language: str = Field(
        default="auto", min_length=1, max_length=50, pattern=r"^[a-zA-Z0-9_+#. \-]+$"
    )
    client_request_id: UUID | None = None


class SessionResponse(BaseModel):
    login_id: str
    csrf_token: str
    expires_at: float
    source_max_chars: int


class ReviewAccepted(BaseModel):
    review_id: str
    status: ReviewStatus
    client_request_id: str


class ReviewDetail(ReviewAccepted):
    language: str
    source_code: str
    review_result: str | None
    error_code: str | None
    error_message: str | None
    model_id: str | None
    model_revision: str | None
    retry_count: int
    created_at: float
    updated_at: float


class ReviewSummary(BaseModel):
    review_id: str
    language: str
    status: ReviewStatus
    error_code: str | None
    created_at: float
    updated_at: float


class HistoryPage(BaseModel):
    items: list[ReviewSummary]
    next_cursor: str | None


class ErrorDetail(BaseModel):
    code: str
    message: str
    request_id: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


# Shared failure shapes, including the custom 422 rather than FastAPI's default detail list.
API_ERROR_RESPONSES = {
    status: {"model": ErrorResponse}
    for status in (400, 401, 403, 404, 409, 413, 415, 422, 429, 500, 503)
}
