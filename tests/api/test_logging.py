import json
import logging
import re
import sqlite3
import threading
from uuid import uuid4

import pytest

from app.coordinator import milliseconds_since
from app.errors import AppError
from app.logging import SafeFormatter
from backend.tests.support import FakeReviewClient, register, wait_ready, wait_review


@pytest.mark.unit
def test_generation_metric_formatter_keeps_only_safe_fixed_fields():
    record = logging.LogRecord(
        "review",
        logging.INFO,
        __file__,
        1,
        "model_generation_finished",
        (),
        None,
    )
    record.duration_ms = 123
    record.generated_tokens = 24
    diagnostics = {
        "section_generation_ms": {"summary": 50, "findings": 73, "suggestions": None},
        "section_first_token_ms": {"summary": 10, "findings": 20, "suggestions": None},
        "section_input_tokens": {"summary": 40, "findings": 45, "suggestions": None},
    }
    for key, value in diagnostics.items():
        setattr(record, key, value)
    record.output_limit_reached = False
    record.output_token_limit = 384
    record.section_generated_tokens = {"summary": 7, "findings": 8, "suggestions": 9}
    record.section_limits_reached = {
        "summary": False,
        "findings": False,
        "suggestions": False,
    }
    record.section_trailing_fragments_removed = {
        "summary": True,
        "findings": False,
        "suggestions": True,
    }
    record.source = "PRIVATE_SOURCE_SENTINEL"
    record.model_output = "MODEL_OUTPUT_SENTINEL"
    record.token_ids = [101, 202, 303]
    record.generation_seed = 42
    record.source_hash = "SOURCE_HASH_SENTINEL"

    payload = json.loads(
        SafeFormatter(environment="local", release_sha="abcdef012345").format(record)
    )

    timestamp = payload.pop("timestamp")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", timestamp)
    assert payload == {
        "level": "INFO",
        "service": "review-backend",
        "environment": "local",
        "release_sha": "abcdef012345",
        "event": "model_generation_finished",
        "duration_ms": 123,
        **diagnostics,
        "generated_tokens": 24,
        "output_limit_reached": False,
        "output_token_limit": 384,
        "section_generated_tokens": {"summary": 7, "findings": 8, "suggestions": 9},
        "section_limits_reached": {
            "summary": False,
            "findings": False,
            "suggestions": False,
        },
        "section_trailing_fragments_removed": {
            "summary": True,
            "findings": False,
            "suggestions": True,
        },
    }
    assert "PRIVATE_SOURCE_SENTINEL" not in json.dumps(payload)
    assert "MODEL_OUTPUT_SENTINEL" not in json.dumps(payload)
    assert "101" not in json.dumps(payload)
    assert "generation_seed" not in payload
    assert "source_hash" not in payload


@pytest.mark.unit
def test_formatter_drops_unknown_validation_reason():
    record = logging.LogRecord("review", logging.INFO, __file__, 1, "review_finished", (), None)
    record.validation_reason = "PRIVATE_UNBOUNDED_REASON"

    payload = json.loads(SafeFormatter(environment="test").format(record))

    assert "validation_reason" not in payload


@pytest.mark.component
def test_http_log_uses_monotonic_duration_and_route_template(factory, monkeypatch, capsys):
    client = factory()
    register(client)
    capsys.readouterr()
    ticks = iter([100.0, 100.125])
    monkeypatch.setattr("app.api.middleware.monotonic", lambda: next(ticks))
    real_review_id = str(uuid4())

    response = client.get(f"/api/v1/reviews/{real_review_id}?secret=PRIVATE_QUERY")

    assert response.status_code == 404
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    request_event = next(event for event in events if event["event"] == "http_request")
    assert request_event["method"] == "GET"
    assert request_event["route"] == "/api/v1/reviews/{review_id}"
    assert request_event["duration_ms"] == 125
    assert request_event["error_code"] == "review_not_found"
    serialized = json.dumps(request_event)
    assert real_review_id not in serialized
    assert "PRIVATE_QUERY" not in serialized


@pytest.mark.component
def test_successful_health_probes_are_suppressed_but_failures_are_logged(factory, capsys):
    client = factory()
    capsys.readouterr()

    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 200
    assert "http_request" not in capsys.readouterr().err

    release = threading.Event()
    entered = threading.Event()

    class ConnectingClient(FakeReviewClient):
        def connect(self):
            entered.set()
            assert release.wait(5)

    loading = factory(ConnectingClient(), ready=False)
    try:
        # Account-startup logs must finish before asserting the last HTTP event.
        assert entered.wait(2)
        capsys.readouterr()
        assert loading.get("/health/ready").status_code == 503
        events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    finally:
        release.set()
        wait_ready(loading)
    assert [event["route"] for event in events if event["event"] == "http_request"] == [
        "/health/ready"
    ]
    assert events[-1]["error_code"] == "inference_connecting"


@pytest.mark.component
def test_review_lifecycle_logs_safe_queue_metrics(factory, capsys):
    client = factory()
    register(client)
    capsys.readouterr()

    submitted = client.post(
        "/api/v1/reviews", json={"source_code": "PRIVATE_SOURCE_SENTINEL"}
    ).json()
    wait_review(client, submitted["review_id"])
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    lifecycle = [
        event
        for event in events
        if event["event"] in {"review_submitted", "review_started", "review_finished"}
    ]

    assert [event["event"] for event in lifecycle] == [
        "review_submitted",
        "review_started",
        "review_finished",
    ]
    assert lifecycle[0]["queue_depth"] == 1
    assert lifecycle[1]["queue_depth"] == 0
    assert lifecycle[1]["queue_wait_ms"] >= 0
    assert lifecycle[2]["duration_ms"] >= lifecycle[1]["queue_wait_ms"]
    assert "PRIVATE_SOURCE_SENTINEL" not in json.dumps(events)


@pytest.mark.component
def test_queue_rejection_log_has_only_bounded_fields(factory, monkeypatch, capsys):
    client = factory()
    register(client)
    capsys.readouterr()

    def reject(*args, **kwargs):
        raise AppError("queue_full", "The review queue is full. Try again later.", 429)

    monkeypatch.setattr(client.app.state.store, "create_review", reject)
    monkeypatch.setattr(client.app.state.store, "queue_depth", lambda: 8)
    response = client.post("/api/v1/reviews", json={"source_code": "PRIVATE_REJECTED_SOURCE"})

    assert response.status_code == 429
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    rejected = next(event for event in events if event["event"] == "queue_rejected")
    assert rejected["queue_depth"] == 8
    assert rejected["outcome"] == "rejected"
    assert rejected["error_code"] == "queue_full"
    assert "PRIVATE_REJECTED_SOURCE" not in json.dumps(events)


@pytest.mark.component
def test_queue_metric_failure_does_not_change_review_or_rejection_behavior(
    factory, monkeypatch, capsys
):
    client = factory()
    register(client)

    def broken_depth():
        raise sqlite3.OperationalError("PRIVATE_DATABASE_PATH")

    monkeypatch.setattr(client.app.state.store, "queue_depth", broken_depth)
    completed = client.post("/api/v1/reviews", json={"source_code": "x = 1"}).json()
    assert wait_review(client, completed["review_id"])["status"] == "completed"

    def reject(*args, **kwargs):
        raise AppError("queue_full", "The review queue is full. Try again later.", 429)

    monkeypatch.setattr(client.app.state.store, "create_review", reject)
    response = client.post("/api/v1/reviews", json={"source_code": "y = 2"})
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]

    assert response.status_code == 429
    rejected = next(event for event in events if event["event"] == "queue_rejected")
    assert "queue_depth" not in rejected
    assert "PRIVATE_DATABASE_PATH" not in json.dumps(events)


@pytest.mark.unit
@pytest.mark.recovery
def test_persisted_queue_wait_calculation_is_clamped_and_exact():
    assert milliseconds_since(100.0, now=100.125) == 125
    assert milliseconds_since(101.0, now=100.0) == 0


@pytest.mark.component
def test_unexpected_errors_do_not_escape_to_transport_logs(factory, monkeypatch, capsys):
    client = factory()
    register(client)

    def broken(*args):
        raise RuntimeError("source-and-password-must-not-escape")

    monkeypatch.setattr(client.app.state.store, "create_review", broken)
    response = client.post("/api/v1/reviews", json={"source_code": "private source"})
    assert response.status_code == 500
    assert "source-and-password" not in response.text + capsys.readouterr().err
