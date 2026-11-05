"""Observed lifecycle behavior with controlled worker exits, real SQLite and HTTP."""

import asyncio
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.coordinator import Coordinator, CoordinatorState
from app.errors import InferenceUnavailable
from app.persistence.storage import Store
from backend.tests.support import FakeReviewClient, register, wait_ready, wait_review


def wait_status(client, status):
    for _ in range(200):
        response = client.get("/health/ready")
        if response.json()["status"] == status:
            return response
        time.sleep(0.01)
    raise AssertionError(f"Readiness did not become {status}")


class ControlledReviewClient(FakeReviewClient):
    def __init__(self, *, late_error=False):
        super().__init__()
        self.release = threading.Event()
        self.started = threading.Event()
        self.late_error = late_error

    def review(self, source, language, stop):
        self.calls += 1
        self.active += 1
        self.peak = max(self.peak, self.active)
        try:
            if self.calls == 1:
                self.started.set()
                assert self.release.wait(5)
                if self.late_error:
                    raise RuntimeError("PRIVATE_LATE_FAILURE")
            return "Review complete."
        finally:
            self.active -= 1


def queue_two_users(client, review_client):
    register(client)
    other = TestClient(
        client.app, base_url="https://testserver", headers={"Origin": "https://testserver"}
    )
    register(other, "bob")
    first = client.post("/api/v1/reviews", json={"source_code": "first"})
    assert first.status_code == 202
    assert review_client.started.wait(2)
    second = other.post("/api/v1/reviews", json={"source_code": "second"})
    assert second.status_code == 202
    return first.json()["review_id"], second.json()["review_id"], other


@pytest.mark.component
@pytest.mark.recovery
@pytest.mark.parametrize("late_error", [False, True])
def test_drain_blocks_next_job_then_recovers_without_overwriting_timeout(factory, late_error):
    review_client = ControlledReviewClient(late_error=late_error)
    client = factory(review_client, inference_timeout_seconds=0.3, inference_drain_seconds=2)
    try:
        first_id, second_id, other = queue_two_users(client, review_client)
        failed = wait_review(client, first_id)
        assert failed["error_code"] == "inference_timeout"
        assert wait_status(client, "inference_draining").status_code == 503
        assert client.get("/health/live").status_code == 200
        assert other.get(f"/api/v1/reviews/{second_id}").json()["status"] == "queued"
        assert client.post("/api/v1/reviews", json={"source_code": "third"}).status_code == 503
        assert review_client.calls == 1
        review_client.release.set()
        assert wait_review(other, second_id)["status"] == "completed"
        wait_ready(client)
        assert client.get(f"/api/v1/reviews/{first_id}").json() == failed
        assert review_client.calls == 2 and review_client.peak == 1
    finally:
        review_client.release.set()


@pytest.mark.component
def test_stuck_worker_remains_unhealthy_after_late_exit_and_leaves_queue_intact(factory):
    review_client = ControlledReviewClient()
    client = factory(review_client, inference_timeout_seconds=0.3, inference_drain_seconds=0.05)
    try:
        first_id, second_id, other = queue_two_users(client, review_client)
        assert wait_review(client, first_id)["error_code"] == "inference_timeout"
        assert wait_status(client, "inference_stuck").status_code == 503
        assert client.get("/health/live").status_code == 503
        assert client.post("/api/v1/reviews", json={"source_code": "next"}).status_code == 503
        review_client.release.set()
        for _ in range(200):
            if review_client.active == 0:
                break
            time.sleep(0.01)
        assert review_client.active == 0 and review_client.calls == 1
        assert client.get("/health/ready").json()["status"] == "inference_stuck"
        assert other.get(f"/api/v1/reviews/{second_id}").json()["status"] == "queued"
    finally:
        review_client.release.set()


@pytest.mark.component
@pytest.mark.recovery
def test_shutdown_during_inference_connect_cannot_reopen_admission(factory):
    release = threading.Event()
    started = threading.Event()

    class ConnectingClient(FakeReviewClient):
        def connect(self):
            started.set()
            assert release.wait(5)

    client = factory(ConnectingClient(), ready=False)
    try:
        assert started.wait(2)
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").json()["status"] == "inference_connecting"
        coordinator = client.app.state.coordinator

        async def close_after_releasing_connect():
            close = asyncio.create_task(coordinator.close())
            await asyncio.sleep(0)
            release.set()
            await close

        client.portal.call(close_after_releasing_connect)
        assert client.get("/health/ready").status_code == 503
        assert client.get("/health/live").status_code == 200
        register(client)
        assert client.post("/api/v1/reviews", json={"source_code": "x"}).status_code == 503
    finally:
        release.set()


@pytest.mark.component
@pytest.mark.recovery
def test_startup_outage_preserves_jobs_and_recovers_without_reinitializing_storage(
    factory, tmp_path, monkeypatch
):
    store = Store(tmp_path)
    store.initialize()
    queued = store.create_review("existing-user", "existing-request", "python", "queued", 8).review
    calls = []
    initialize, recover = Store.initialize, Store.recover

    def initialize_once(current):
        calls.append("initialize")
        return initialize(current)

    def recover_once(current, *args):
        calls.append("recover")
        return recover(current, *args)

    class RecoverableClient(FakeReviewClient):
        available = threading.Event()
        checked = threading.Event()

        def connect(self):
            super().connect()
            if not self.available.is_set():
                self.checked.set()
                raise InferenceUnavailable()

    monkeypatch.setattr(Store, "initialize", initialize_once)
    monkeypatch.setattr(Store, "recover", recover_once)
    monkeypatch.setattr("app.coordinator.INFERENCE_RETRY_INITIAL_SECONDS", 0.01)
    monkeypatch.setattr("app.coordinator.INFERENCE_RETRY_MAX_SECONDS", 0.02)
    review_client = RecoverableClient()
    client = factory(review_client, ready=False)
    assert wait_status(client, "inference_connecting").status_code == 503
    assert review_client.checked.wait(2)
    assert client.get("/health/live").status_code == 200
    assert client.get("/api/v1/runtime").json()["accepting_submissions"] is False
    assert store.get_review("existing-user", queued["review_id"])["status"] == "queued"
    register(client)
    assert client.post("/api/v1/reviews", json={"source_code": "new"}).status_code == 503
    review_client.available.set()
    wait_ready(client)
    for _ in range(200):
        result = store.get_review("existing-user", queued["review_id"])
        if result["status"] == "completed":
            break
        time.sleep(0.01)
    assert result["status"] == "completed"
    assert result["retry_count"] == 0
    assert review_client.calls == 1
    assert review_client.connections >= 2
    assert calls == ["initialize", "recover"]


@pytest.mark.component
@pytest.mark.recovery
def test_health_checks_pause_queued_jobs_during_an_outage_and_resume_after_reconnect(
    factory, monkeypatch
):
    class RecoverableClient(ControlledReviewClient):
        available = threading.Event()

        def connect(self):
            if not self.available.is_set():
                raise InferenceUnavailable()

    monkeypatch.setattr("app.coordinator.INFERENCE_HEALTH_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr("app.coordinator.INFERENCE_RETRY_INITIAL_SECONDS", 0.01)
    monkeypatch.setattr("app.coordinator.INFERENCE_RETRY_MAX_SECONDS", 0.02)
    review_client = RecoverableClient()
    review_client.available.set()
    client = factory(review_client, inference_timeout_seconds=2)
    try:
        first_id, second_id, other = queue_two_users(client, review_client)
        review_client.available.clear()
        review_client.release.set()
        assert wait_review(client, first_id)["status"] == "completed"
        assert wait_status(client, "inference_connecting").status_code == 503
        assert client.get("/health/live").status_code == 200
        assert other.get(f"/api/v1/reviews/{second_id}").json()["status"] == "queued"
        assert review_client.calls == 1
        review_client.available.set()
        assert wait_review(other, second_id)["status"] == "completed"
        assert review_client.calls == 2 and review_client.peak == 1
        wait_ready(client)
    finally:
        review_client.release.set()


@pytest.mark.component
@pytest.mark.recovery
def test_reconnect_uses_capped_backoff_and_only_repeats_the_handshake(factory, monkeypatch):
    waits = []
    original_wait = Coordinator.wait_for_shutdown

    class FlakyClient(FakeReviewClient):
        def connect(self):
            super().connect()
            if self.connections <= 6:
                raise InferenceUnavailable()

    async def advance_retry(coordinator, delay):
        if coordinator.state is CoordinatorState.CONNECTING:
            waits.append(delay)
            await asyncio.sleep(0)
        else:
            await original_wait(coordinator, delay)

    monkeypatch.setattr(Coordinator, "wait_for_shutdown", advance_retry)
    review_client = FlakyClient()
    client = factory(review_client)
    assert waits == [1, 2, 4, 8, 10, 10]
    assert review_client.connections == 7
    assert review_client.calls == 0
    assert client.get("/health/ready").status_code == 200


@pytest.mark.component
@pytest.mark.recovery
def test_shutdown_interrupts_reconnect_backoff_and_close_is_idempotent(factory, monkeypatch):
    waiting = threading.Event()
    original_wait = Coordinator.wait_for_shutdown

    class OfflineClient(FakeReviewClient):
        def connect(self):
            super().connect()
            raise InferenceUnavailable()

    async def observe_wait(coordinator, delay):
        waiting.set()
        await original_wait(coordinator, delay)

    monkeypatch.setattr("app.coordinator.INFERENCE_RETRY_INITIAL_SECONDS", 10)
    monkeypatch.setattr(Coordinator, "wait_for_shutdown", observe_wait)
    review_client = OfflineClient()
    client = factory(review_client, ready=False)
    assert waiting.wait(2)
    started = time.monotonic()
    client.portal.call(client.app.state.coordinator.close)
    client.portal.call(client.app.state.coordinator.close)
    assert time.monotonic() - started < 1
    assert review_client.connections == 1 and review_client.closed
    assert client.app.state.coordinator.task.done()
    assert client.get("/health/ready").status_code == 503


@pytest.mark.component
@pytest.mark.recovery
def test_known_dependency_failure_reconnects_without_replaying_the_failed_review(factory):
    review_client = FakeReviewClient(result=InferenceUnavailable())
    client = factory(review_client)
    register(client)
    first = client.post("/api/v1/reviews", json={"source_code": "first"}).json()
    failed = wait_review(client, first["review_id"])
    assert failed["error_code"] == "inference_unavailable"
    wait_ready(client)
    assert review_client.calls == 1 and review_client.connections == 2
    review_client.result = "Recovered review."
    second = client.post("/api/v1/reviews", json={"source_code": "second"}).json()
    assert wait_review(client, second["review_id"])["status"] == "completed"
    assert review_client.calls == 2
    assert client.get(f"/api/v1/reviews/{first['review_id']}").json() == failed
