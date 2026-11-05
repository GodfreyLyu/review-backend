"""Review admission orchestration; the store owns one atomic admission transaction."""

from typing import Protocol
from uuid import uuid4

from app.domain import CreateReviewResult, ReviewRecord
from app.errors import AppError


class AdmissionStore(Protocol):
    def create_review(
        self, user_id: str, request_id: str, language: str, source: str, capacity: int, **policy
    ) -> CreateReviewResult: ...


def validate_source(source: str, max_chars: int) -> None:
    if not source.strip():
        raise AppError("empty_input", "Enter source code before running a review.", 422)
    if len(source) > max_chars:
        raise AppError("input_too_large", f"Use at most {max_chars} characters.", 422)


class ReviewService:
    def __init__(
        self,
        store: AdmissionStore,
        *,
        max_chars: int,
        capacity: int,
        deadline_seconds: float,
        active_limit: int,
        submission_limit: int,
        metrics=None,
    ):
        self.store = store
        self.max_chars = max_chars
        self.capacity = capacity
        self.deadline_seconds = deadline_seconds
        self.active_limit = active_limit
        self.submission_limit = submission_limit
        self.metrics = metrics

    def submit(
        self,
        user_id: str,
        source: str,
        language: str,
        request_id: str | None = None,
        *,
        trace_id: str | None = None,
    ) -> ReviewRecord:
        validate_source(source, self.max_chars)
        try:
            result = self.store.create_review(
                user_id,
                request_id or str(uuid4()),
                language,
                source,
                self.capacity,
                trace_id=trace_id,
                deadline_seconds=self.deadline_seconds,
                max_active_per_user=self.active_limit,
                submission_limit=self.submission_limit,
            )
        except AppError as exc:
            if self.metrics:
                self.metrics.rejected(exc.code)
            raise
        if result.created and self.metrics:
            self.metrics.submitted()
        return result.review
