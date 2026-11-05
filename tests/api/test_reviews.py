import json
import sqlite3
from uuid import uuid4

import pytest

from app.errors import AppError, ValidationReason
from app.inference.review_output import INVALID_REVIEW_MESSAGE, validate_review_output
from app.logging import SERVICE_NAME
from backend.tests.support import FakeReviewClient, register, wait_review


@pytest.mark.component
@pytest.mark.recovery
def test_idempotency_conflicts_and_completed_retry(factory):
    review_client = FakeReviewClient(delay=0.1)
    client = factory(review_client)
    register(client)
    payload = {"source_code": "hello arbitrary source", "client_request_id": str(uuid4())}
    first = client.post("/api/v1/reviews", json=payload)
    again = client.post("/api/v1/reviews", json=payload)
    assert first.status_code == again.status_code == 202
    assert first.json()["review_id"] == again.json()["review_id"]
    assert (
        client.post("/api/v1/reviews", json=payload | {"source_code": "different"}).status_code
        == 409
    )
    wait_review(client, first.json()["review_id"])
    assert client.post("/api/v1/reviews", json=payload).json()["status"] == "completed"
    assert review_client.calls == 1


@pytest.mark.component
@pytest.mark.parametrize(
    "code,error",
    [("", "validation_error"), (" \n\t", "empty_input"), ("x" * 121, "input_too_large")],
)
def test_input_limits(factory, code, error):
    client = factory(source_max_chars=120)
    register(client)
    response = client.post("/api/v1/reviews", json={"source_code": code})
    assert response.status_code == 422 and response.json()["error"]["code"] == error


@pytest.mark.component
@pytest.mark.security
def test_invalid_id_body_limit_and_no_sensitive_echo(factory, capsys):
    client = factory()
    register(client)
    assert client.get("/api/v1/reviews/not-a-uuid").status_code == 422
    assert client.get(f"/api/v1/reviews/{uuid4()}").status_code == 404
    secret = "sensitive-password-or-source"
    response = client.post("/api/v1/reviews", json={"source_code": {"secret": secret}})
    assert secret not in response.text
    assert client.post("/api/v1/reviews", json={"source_code": "x" * 140000}).status_code == 413
    assert secret not in capsys.readouterr().err
    assert response.headers["x-request-id"] and response.headers["cache-control"] == "no-store"


@pytest.mark.component
@pytest.mark.parametrize(
    "result,code",
    [
        ("", "empty_model_response"),
        (RuntimeError("secret details"), "inference_failed"),
        (
            AppError(
                "invalid_model_response",
                "The model returned an unusable review. Try a smaller, self-contained snippet.",
                502,
            ),
            "invalid_model_response",
        ),
    ],
)
def test_model_failure_translation(factory, result, code):
    client = factory(FakeReviewClient(result=result))
    register(client)
    job = client.post("/api/v1/reviews", json={"source_code": "x"}).json()
    review = wait_review(client, job["review_id"])
    assert review["status"] == "failed" and review["error_code"] == code
    assert "secret details" not in str(review)


@pytest.mark.component
@pytest.mark.contract
def test_invalid_model_response_public_contract_and_safe_validation_log(factory, capsys):
    class ValidatingReviewClient(FakeReviewClient):
        def review(self, source, language, stop):
            return validate_review_output(self.result, source)

    source = "def private_source_identifier(private_values): return private_values"
    rejected_output = (
        "## Summary\nMODEL_OUTPUT_SENTINEL generic routine.\n"
        "## Findings\nNo issue.\n## Suggestions\nAdd tests."
    )
    client = factory(ValidatingReviewClient(result=rejected_output))
    register(client)
    capsys.readouterr()

    job = client.post("/api/v1/reviews", json={"source_code": source}).json()
    review = wait_review(client, job["review_id"])
    log_lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    finished = [event for event in log_lines if event["event"] == "review_finished"]

    assert review["status"] == "failed"
    assert review["error_code"] == "invalid_model_response"
    assert review["error_message"] == INVALID_REVIEW_MESSAGE
    assert review["review_result"] is None
    assert "validation_reason" not in review
    assert len(finished) == 1
    assert {
        "level": "INFO",
        "service": SERVICE_NAME,
        "environment": "test",
        "event": "review_finished",
        "review_id": job["review_id"],
        "error_code": "invalid_model_response",
        "outcome": "invalid_model_response",
        "validation_reason": "detached_source",
    }.items() <= finished[0].items()
    assert finished[0]["duration_ms"] >= 0
    serialized_logs = json.dumps(log_lines)
    assert "private_source_identifier" not in serialized_logs
    assert "private_values" not in serialized_logs
    assert "MODEL_OUTPUT_SENTINEL" not in serialized_logs


@pytest.mark.component
def test_truncated_section_keeps_public_error_generic_and_logs_only_fixed_reason(factory, capsys):
    source_sentinel = "PRIVATE_TRUNCATED_SOURCE_SENTINEL"
    model_output_sentinel = "PRIVATE_TRUNCATED_MODEL_OUTPUT_SENTINEL"
    error = AppError(
        "invalid_model_response",
        INVALID_REVIEW_MESSAGE,
        502,
        validation_reason=ValidationReason.TRUNCATED_SECTION,
    )
    client = factory(FakeReviewClient(result=error))
    register(client)
    capsys.readouterr()

    job = client.post("/api/v1/reviews", json={"source_code": source_sentinel}).json()
    review = wait_review(client, job["review_id"])
    log_lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    finished = [event for event in log_lines if event["event"] == "review_finished"]

    assert review["status"] == "failed"
    assert review["error_code"] == "invalid_model_response"
    assert review["error_message"] == INVALID_REVIEW_MESSAGE
    assert review["review_result"] is None
    assert "validation_reason" not in review
    assert len(finished) == 1
    assert {
        "level": "INFO",
        "service": SERVICE_NAME,
        "environment": "test",
        "event": "review_finished",
        "review_id": job["review_id"],
        "error_code": "invalid_model_response",
        "outcome": "invalid_model_response",
        "validation_reason": "truncated_section",
    }.items() <= finished[0].items()
    assert finished[0]["duration_ms"] >= 0
    serialized_logs = json.dumps(log_lines)
    assert source_sentinel not in serialized_logs
    assert model_output_sentinel not in serialized_logs


@pytest.mark.component
def test_inference_timeout(factory):
    review_client = FakeReviewClient(delay=1)
    client = factory(review_client, inference_timeout_seconds=0.03)
    register(client)
    job = client.post("/api/v1/reviews", json={"source_code": "x"}).json()
    assert wait_review(client, job["review_id"])["error_code"] == "inference_timeout"
    assert review_client.peak == 1 and client.get("/health/live").status_code == 200


@pytest.mark.component
def test_history_read_and_save_failure(factory, monkeypatch):
    client = factory()
    register(client)

    def broken(*args, **kwargs):
        raise sqlite3.OperationalError("sensitive filesystem path")

    monkeypatch.setattr(client.app.state.store, "history", broken)
    response = client.get("/api/v1/reviews")
    assert response.status_code == 503 and "sensitive" not in response.text
    monkeypatch.setattr(client.app.state.store, "create_review", broken)
    assert client.post("/api/v1/reviews", json={"source_code": "x"}).status_code == 503


@pytest.mark.component
def test_routers_keep_each_application_settings_and_storage_isolated(factory, tmp_path):
    first = factory(data_dir=tmp_path / "first", source_max_chars=3)
    second = factory(data_dir=tmp_path / "second", source_max_chars=100)
    register(first, "alice")
    register(second, "bob")
    assert first.get("/api/v1/auth/me").json()["source_max_chars"] == 3
    assert second.get("/api/v1/auth/me").json()["source_max_chars"] == 100
    payload = {"source_code": "value = 1"}
    assert first.post("/api/v1/reviews", json=payload).status_code == 422
    response = second.post("/api/v1/reviews", json=payload)
    assert response.status_code == 202
    review_id = response.json()["review_id"]
    assert wait_review(second, review_id)["status"] == "completed"
    assert first.get("/api/v1/reviews").json()["items"] == []
    assert first.get(f"/api/v1/reviews/{review_id}").status_code == 404
