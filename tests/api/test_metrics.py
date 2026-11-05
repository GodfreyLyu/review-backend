"""Prometheus contracts use temporary storage and controlled model doubles only."""

import sqlite3
import threading
import time
from uuid import uuid4

import pytest
from prometheus_client import CONTENT_TYPE_LATEST
from prometheus_client.parser import text_string_to_metric_families

from app.coordinator import CoordinatorState
from app.errors import AppError
from app.inference.generation import GenerationMetrics, SectionMetrics
from app.monitoring import COORDINATOR_STATES, Metrics
from app.persistence.storage import Store
from backend.tests.support import FakeReviewClient, register, wait_review


def samples(text):
    return [sample for family in text_string_to_metric_families(text) for sample in family.samples]


def value(metrics, name, labels=None):
    return metrics.registry.get_sample_value(name, labels or {})


def wait_value(metrics, name, expected, labels=None):
    for _ in range(200):
        if value(metrics, name, labels) == expected:
            return
        time.sleep(0.01)
    raise AssertionError(f"Metric {name} did not become {expected}")


@pytest.mark.component
@pytest.mark.contract
def test_anonymous_scrape_has_no_http_metrics_or_log_side_effects(factory, capsys):
    client = factory()
    before = client.app.state.metrics.render()
    capsys.readouterr()

    response = client.get("/metrics?secret=PRIVATE_QUERY")
    again = client.get("/metrics")

    assert response.status_code == again.status_code == 200
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST
    assert response.headers["cache-control"] == "no-store"
    assert response.content == before == again.content
    assert "http_request" not in capsys.readouterr().err
    assert "PRIVATE_QUERY" not in response.text
    assert "process_" not in response.text and "python_info" not in response.text


@pytest.mark.component
@pytest.mark.security
def test_labels_use_templates_and_bounded_fallbacks(factory):
    client = factory()
    metrics = client.app.state.metrics
    register(client)
    review_id = str(uuid4())
    client.get(f"/api/v1/reviews/{review_id}?secret=PRIVATE_QUERY")
    client.get("/PRIVATE_PATH")
    client.request("PRIVATE_METHOD", "/api/v1/runtime")
    metrics.rejected("PRIVATE_REASON")
    metrics.finished("PRIVATE_OUTCOME", 0.1)

    exposition = client.get("/metrics").text

    assert "PRIVATE" not in exposition and review_id not in exposition
    assert (
        value(
            metrics,
            "review_http_requests_total",
            {
                "method": "GET",
                "route": "/api/v1/reviews/{review_id}",
                "status": "404",
            },
        )
        == 1
    )
    assert (
        value(
            metrics,
            "review_http_requests_total",
            {
                "method": "GET",
                "route": "unknown",
                "status": "404",
            },
        )
        == 1
    )
    assert (
        value(
            metrics,
            "review_http_requests_total",
            {
                "method": "OTHER",
                "route": "/api/v1/runtime",
                "status": "405",
            },
        )
        == 1
    )
    assert value(metrics, "review_rejections_total", {"reason": "internal_error"}) == 1
    assert value(metrics, "review_completions_total", {"outcome": "inference_failed"}) == 1


@pytest.mark.component
def test_application_registries_are_isolated(factory, tmp_path):
    first = factory(data_dir=tmp_path / "first")
    second = factory(data_dir=tmp_path / "second")
    first.app.state.metrics.submitted()
    assert value(first.app.state.metrics, "review_submissions_total") == 1
    assert value(second.app.state.metrics, "review_submissions_total") == 0


@pytest.mark.component
@pytest.mark.contract
def test_event_counters_have_zero_baselines_before_the_first_review(factory):
    client = factory()
    metrics = client.app.state.metrics
    assert value(metrics, "review_completions_total", {"outcome": "completed"}) == 0
    assert value(metrics, "review_completions_total", {"outcome": "inference_timeout"}) == 0
    assert value(metrics, "review_rejections_total", {"reason": "queue_full"}) == 0
    assert value(metrics, "review_rejections_total", {"reason": "rate_limited"}) == 0
    assert value(metrics, "review_generated_tokens_total", {"section": "summary"}) is None
    assert (
        value(metrics, "review_first_content_duration_seconds_count", {"section": "summary"}) == 0
    )
    assert value(metrics, "review_generation_duration_seconds_count", {"section": "summary"}) == 0


@pytest.mark.component
@pytest.mark.recovery
def test_idempotent_submission_counts_one_admission_and_terminal_result(factory):
    client = factory()
    register(client)
    payload = {"source_code": "PRIVATE_SOURCE", "client_request_id": str(uuid4())}
    first = client.post("/api/v1/reviews", json=payload)
    wait_review(client, first.json()["review_id"])
    second = client.post("/api/v1/reviews", json=payload)
    metrics = client.app.state.metrics

    assert first.status_code == second.status_code == 202
    assert first.json()["review_id"] == second.json()["review_id"]
    assert value(metrics, "review_submissions_total") == 1
    wait_value(metrics, "review_completions_total", 1, {"outcome": "completed"})
    assert value(metrics, "review_queue_wait_seconds_count") == 1
    assert value(metrics, "review_execution_duration_seconds_count") == 1
    assert value(metrics, "review_worker_busy") == 0
    assert "PRIVATE_SOURCE" not in metrics.render().decode()


@pytest.mark.component
@pytest.mark.parametrize("reason", ["queue_full", "review_active", "idempotency_conflict"])
def test_rejected_admission_does_not_count_as_submission(factory, monkeypatch, reason):
    client = factory()
    register(client)

    def reject(*args):
        raise AppError(reason, "Submission rejected.", 429)

    monkeypatch.setattr(client.app.state.store, "create_review", reject)
    response = client.post("/api/v1/reviews", json={"source_code": "x = 1"})
    assert response.status_code == 429
    assert value(client.app.state.metrics, "review_submissions_total") == 0
    assert value(client.app.state.metrics, "review_rejections_total", {"reason": reason}) == 1


@pytest.mark.unit
@pytest.mark.recovery
def test_queue_scrape_does_not_create_storage_or_fabricate_empty_queue(tmp_path):
    store = Store(tmp_path)
    metrics = Metrics(store, 8)
    assert value(metrics, "review_metrics_collection_success") == 0
    assert value(metrics, "review_queue_depth") is None
    assert value(metrics, "review_queue_oldest_age_seconds") is None
    assert value(metrics, "review_queue_capacity") == 8
    assert not store.path.exists()


@pytest.mark.component
@pytest.mark.recovery
def test_queue_snapshot_survives_restart_and_never_pings_storage(tmp_path, monkeypatch):
    store = Store(tmp_path)
    store.initialize()
    store.create_review("alice", "request-one", "python", "x", 8)
    with store.connection(write=True) as db:
        db.execute("UPDATE reviews SET created_at = ?", (time.time() - 90,))
        db.execute("INSERT INTO sessions VALUES ('token', 'alice', 'alice', 'csrf', 0)")

    def forbidden_ping():
        pytest.fail("Metrics must not run a writable readiness probe")

    monkeypatch.setattr(store, "ping", forbidden_ping)
    metrics = Metrics(store, 8)
    assert value(metrics, "review_queue_depth") == 1
    assert value(metrics, "review_queue_oldest_age_seconds") >= 90
    assert value(metrics, "review_submissions_total") == 0
    with store.connection() as db:
        assert db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1
    job = store.claim()
    store.recover(1, 8)
    assert value(metrics, "review_queue_depth") == 1
    assert value(metrics, "review_queue_oldest_age_seconds") >= 90
    assert job["retry_count"] == 0


@pytest.mark.component
def test_failed_collection_omits_old_values_and_preserves_business(factory, monkeypatch):
    client = factory()
    register(client)

    def broken_snapshot():
        raise sqlite3.OperationalError("PRIVATE_DATABASE_PATH")

    monkeypatch.setattr(client.app.state.store, "queue_snapshot", broken_snapshot)
    response = client.get("/metrics")
    observations = {sample.name: sample.value for sample in samples(response.text)}
    assert response.status_code == 200
    assert observations["review_metrics_collection_success"] == 0
    assert "review_queue_depth" not in observations
    assert "review_queue_oldest_age_seconds" not in observations
    assert "PRIVATE_DATABASE_PATH" not in response.text
    submitted = client.post("/api/v1/reviews", json={"source_code": "x = 1"}).json()
    assert wait_review(client, submitted["review_id"])["status"] == "completed"


@pytest.mark.component
def test_observation_failure_cannot_fail_a_review(factory, monkeypatch):
    client = factory()
    register(client)
    metrics = client.app.state.metrics

    def broken(*args, **kwargs):
        raise RuntimeError("PRIVATE_MONITORING_FAILURE")

    monkeypatch.setattr(metrics.submissions, "inc", broken)
    monkeypatch.setattr(metrics.worker_busy, "set", broken)
    monkeypatch.setattr(metrics.completions, "labels", broken)
    monkeypatch.setattr(metrics.http_requests, "labels", broken)
    submitted = client.post("/api/v1/reviews", json={"source_code": "x = 1"})
    assert submitted.status_code == 202
    assert wait_review(client, submitted.json()["review_id"])["status"] == "completed"


@pytest.mark.component
@pytest.mark.recovery
@pytest.mark.parametrize("stuck", [False, True])
def test_worker_remains_busy_after_timeout_until_the_executor_exits(factory, stuck):
    release = threading.Event()
    entered = threading.Event()

    class ControlledReviewClient(FakeReviewClient):
        def review(self, source, language, stop):
            entered.set()
            assert release.wait(5)
            return "Late result"

    client = factory(
        ControlledReviewClient(),
        inference_timeout_seconds=0.1,
        inference_drain_seconds=0.02 if stuck else 2,
    )
    register(client)
    metrics = client.app.state.metrics
    try:
        submitted = client.post("/api/v1/reviews", json={"source_code": "x = 1"}).json()
        assert entered.wait(2)
        assert wait_review(client, submitted["review_id"])["error_code"] == "inference_timeout"
        state = "inference_stuck" if stuck else "inference_draining"
        wait_value(metrics, "review_coordinator_state", 1, {"state": state})
        assert value(metrics, "review_worker_busy") == 1
        assert value(metrics, "review_execution_duration_seconds_count") == 0
        assert value(metrics, "review_completions_total", {"outcome": "inference_timeout"}) == 1
        release.set()
        wait_value(metrics, "review_worker_busy", 0)
        wait_value(metrics, "review_execution_duration_seconds_count", 1)
        final_state = "inference_stuck" if stuck else "ready"
        wait_value(metrics, "review_coordinator_state", 1, {"state": final_state})
        assert value(metrics, "review_execution_duration_seconds_sum") >= 0.1
        assert value(metrics, "review_completions_total", {"outcome": "completed"}) == 0
    finally:
        release.set()


@pytest.mark.component
@pytest.mark.parametrize("state", list(CoordinatorState))
def test_coordinator_states_are_one_hot(factory, state):
    client = factory()
    client.app.state.coordinator._transition(state)
    metrics = client.app.state.metrics
    values = {
        name: value(metrics, "review_coordinator_state", {"state": name})
        for name in COORDINATOR_STATES
    }
    assert sum(values.values()) == 1
    assert values[state] == 1


@pytest.mark.unit
def test_completed_generations_distinguish_zero_tokens_from_unobserved_sections(tmp_path):
    metrics = Metrics(Store(tmp_path), 8)
    for section in ("summary", "findings", "suggestions"):
        assert value(metrics, "review_generation_duration_seconds_count", {"section": section}) == 0
        assert (
            value(metrics, "review_first_content_duration_seconds_count", {"section": section}) == 0
        )
    metrics.generation(
        GenerationMetrics(
            output_token_limit=20,
            sections={
                "summary": SectionMetrics(
                    generated_tokens=5,
                    generation_ms=100,
                    first_token_ms=25,
                    input_tokens=40,
                    limit_reached=True,
                ),
                "findings": SectionMetrics(generated_tokens=0, input_tokens=10),
                "PRIVATE_SECTION": SectionMetrics(
                    generated_tokens=3, input_tokens=10, generation_ms=100
                ),
            },
        )
    )
    assert value(metrics, "review_generation_duration_seconds_sum", {"section": "summary"}) == 0.1
    assert value(metrics, "review_generation_duration_seconds_count", {"section": "summary"}) == 1
    assert (
        value(metrics, "review_first_content_duration_seconds_count", {"section": "summary"}) == 1
    )
    assert (
        value(metrics, "review_first_content_duration_seconds_sum", {"section": "summary"}) == 0.025
    )
    assert value(metrics, "review_generated_tokens_total", {"section": "summary"}) == 5
    assert value(metrics, "review_input_tokens_total", {"section": "summary"}) == 40
    assert value(metrics, "review_output_limit_total", {"section": "summary"}) == 1
    assert value(metrics, "review_generated_tokens_total", {"section": "findings"}) == 0
    assert value(metrics, "review_input_tokens_total", {"section": "findings"}) == 10
    assert value(metrics, "review_output_limit_total", {"section": "findings"}) == 0
    assert value(metrics, "review_generated_tokens_total", {"section": "suggestions"}) is None
    assert value(metrics, "review_input_tokens_total", {"section": "suggestions"}) is None
    assert value(metrics, "review_output_limit_total", {"section": "suggestions"}) is None
    assert value(metrics, "review_generation_duration_seconds_count", {"section": "findings"}) == 0
    assert (
        value(metrics, "review_first_content_duration_seconds_count", {"section": "findings"}) == 0
    )
    assert (
        value(metrics, "review_generation_duration_seconds_count", {"section": "suggestions"}) == 0
    )
    assert (
        value(metrics, "review_first_content_duration_seconds_count", {"section": "suggestions"})
        == 0
    )
    assert "PRIVATE_SECTION" not in metrics.render().decode()
