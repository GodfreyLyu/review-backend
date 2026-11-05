"""Review admission policy; persistence owns the atomic admission transaction."""

import logging
import sqlite3
from uuid import uuid4

from app.config import Settings
from app.coordinator import Coordinator
from app.domain import ReviewRecord
from app.errors import AppError
from app.monitoring import Metrics
from app.persistence.storage import Store
from app.rate_limit import RateLimiter

logger = logging.getLogger("review")


class ReviewService:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        coordinator: Coordinator,
        limiter: RateLimiter,
        metrics: Metrics,
    ) -> None:
        self.settings = settings
        self.store = store
        self.coordinator = coordinator
        self.limiter = limiter
        self.metrics = metrics

    def submit(
        self,
        user_id: str,
        source: str,
        language: str,
        request_id: str | None = None,
    ) -> ReviewRecord:
        try:
            self._validate(source)
            self.limiter.check("submit:" + user_id, self.settings.submission_rate_limit)
            result = self.store.create_review(
                user_id,
                request_id or str(uuid4()),
                language,
                source,
                self.settings.queue_capacity,
            )
        except AppError as exc:
            self.metrics.rejected(exc.code)
            if exc.code == "queue_full":
                self._log_queue_rejection()
            raise
        if result.created:
            self.metrics.submitted()
            logger.info(
                "review_submitted",
                extra={
                    "review_id": result.review["review_id"],
                    "outcome": "accepted",
                    "error_code": "none",
                    "queue_depth": result.queue_depth,
                },
            )
        return result.review

    def _validate(self, source: str) -> None:
        if not source.strip():
            raise AppError("empty_input", "Enter source code before running a review.", 422)
        if len(source) > self.settings.source_max_chars:
            raise AppError(
                "input_too_large", f"Use at most {self.settings.source_max_chars} characters.", 422
            )
        if not self.coordinator.ready:
            raise AppError(
                self.coordinator.state,
                "The review service is not ready. Please retry shortly.",
                503,
            )

    def _log_queue_rejection(self) -> None:
        fields: dict[str, str | int] = {"outcome": "rejected", "error_code": "queue_full"}
        try:
            fields["queue_depth"] = self.store.queue_depth()
        except sqlite3.Error:
            # Diagnostics must not replace the original admission failure.
            pass
        logger.warning("queue_rejected", extra=fields)
