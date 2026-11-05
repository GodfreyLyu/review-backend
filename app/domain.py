"""Internal records keep their native dictionaries; HTTP schemas live separately."""

from dataclasses import dataclass
from typing import Literal, NotRequired, TypedDict

ReviewStatus = Literal["queued", "running", "completed", "failed"]


class UserRecord(TypedDict):
    user_id: str
    login_id: str
    password_hash: str
    created_at: str
    disabled: NotRequired[bool]


class SessionRecord(TypedDict):
    token_hash: str
    user_id: str
    login_id: str
    csrf_token: str
    expires_at: float


class ReviewSummaryRecord(TypedDict):
    review_id: str
    language: str
    status: ReviewStatus
    error_code: str | None
    created_at: float
    updated_at: float


class ReviewRecord(ReviewSummaryRecord):
    user_id: str
    client_request_id: str
    source_code: str
    review_result: str | None
    error_message: str | None
    model_id: str | None
    model_revision: str | None
    retry_count: int


class HistoryRecord(TypedDict):
    items: list[ReviewSummaryRecord]
    next_cursor: str | None


@dataclass(frozen=True)
class CreateReviewResult:
    review: ReviewRecord
    created: bool
    # Only a newly admitted review has a queue depth measured in its transaction.
    queue_depth: int | None = None
