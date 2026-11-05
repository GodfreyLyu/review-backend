"""Pure admission policy and API readiness boundaries."""

from unittest.mock import Mock

import pytest

from app.application.admission import ReviewService, validate_source
from app.domain import CreateReviewResult
from app.errors import AppError
from app.health import check_readiness

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "source,limit,code", [("  ", 100, "empty_input"), ("abcd", 3, "input_too_large")]
)
def test_source_policy(source, limit, code):
    with pytest.raises(AppError) as error:
        validate_source(source, limit)
    assert error.value.code == code


def test_admission_delegates_atomic_idempotence_and_limits_without_inference():
    store = Mock()
    row = {"review_id": "saved", "status": "queued"}
    store.create_review.return_value = CreateReviewResult(review=row, created=False)
    service = ReviewService(
        store, max_chars=100, capacity=8, deadline_seconds=60, active_limit=1, submission_limit=6
    )
    assert service.submit("user", "value=1", "python", "key", trace_id="trace") is row
    store.create_review.assert_called_once_with(
        "user",
        "key",
        "python",
        "value=1",
        8,
        trace_id="trace",
        deadline_seconds=60,
        max_active_per_user=1,
        submission_limit=6,
    )


def test_readiness_depends_on_database_only():
    store = Mock()
    assert check_readiness(store).status == "ready"
    store.ping.side_effect = OSError("sensitive connection text")
    result = check_readiness(store)
    assert result.status == "storage_unavailable"
    assert "sensitive" not in repr(result)
