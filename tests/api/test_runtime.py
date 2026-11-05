"""Instance display contracts use temporary stores and model doubles only."""

import sqlite3

import pytest

from app.coordinator import CoordinatorState
from app.inference.identity import model_identity
from app.inference.rpc.client import ThriftReviewClient


@pytest.mark.component
@pytest.mark.parametrize("environment", ["minikube", "development", "unknown"])
def test_runtime_explicit_environment_and_safe_public_fields(factory, environment):
    client = factory(deployment_environment=environment)
    response = client.get("/api/v1/runtime")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "deployment_environment": environment,
        "service_status": "ready",
        "accepting_submissions": True,
        "inference_mode": "simulated",
        "model_id": "Simulated model",
        "model_revision": "fixture-v1",
        "model_source": "test_fixture",
        "device": None,
    }
    assert client.get("/health/ready").json() == {"status": "ready"}
    assert client.get("/api/v1/reviews").status_code == 401


@pytest.mark.component
@pytest.mark.recovery
@pytest.mark.parametrize("state", list(CoordinatorState))
def test_runtime_reuses_readiness_and_recovers(factory, state):
    client = factory()
    coordinator = client.app.state.coordinator
    coordinator._state = state
    data = client.get("/api/v1/runtime").json()
    probe = client.get("/health/ready")
    assert data["service_status"] == probe.json()["status"] == state.value
    assert data["accepting_submissions"] is (probe.status_code == 200)
    coordinator._state = CoordinatorState.READY
    assert client.get("/api/v1/runtime").json()["accepting_submissions"] is True


@pytest.mark.component
@pytest.mark.recovery
def test_storage_failure_and_shutdown_never_claim_readiness(factory, monkeypatch):
    client = factory()

    def fail():
        raise sqlite3.OperationalError("private path must not be returned")

    monkeypatch.setattr(client.app.state.store, "ping", fail)
    data = client.get("/api/v1/runtime").json()
    assert data["service_status"] == "storage_unavailable"
    assert data["accepting_submissions"] is False
    assert "private" not in str(data)
    assert client.get("/health/ready").status_code == 503
    client.app.state.coordinator._closing = True
    assert client.get("/api/v1/runtime").json()["service_status"] == "shutting_down"


@pytest.mark.component
@pytest.mark.contract
def test_real_client_identity_before_connecting_is_not_fabricated(factory):
    client = factory()
    settings = client.app.state.settings
    identity = model_identity(ThriftReviewClient(settings))
    assert identity == {
        "inference_mode": "real",
        "model_id": None,
        "model_revision": None,
        "model_source": "ollama_api",
        "device": None,
        "inference_backend": "ollama",
        "quantization": None,
    }
    # Unknown injected adapters must not be labeled real just because Qwen is configured.
    assert model_identity(object())["inference_mode"] == "unknown"
    assert model_identity(object())["model_id"] is None


@pytest.mark.component
@pytest.mark.parametrize("unavailable", [False, True])
def test_runtime_and_probe_keep_distinct_http_status_and_safe_logs(
    factory, monkeypatch, capsys, unavailable
):
    import json

    client = factory()
    calls = []

    def ping():
        calls.append("ping")
        if unavailable:
            raise sqlite3.OperationalError("PRIVATE_DATABASE_PATH")

    monkeypatch.setattr(client.app.state.store, "ping", ping)
    capsys.readouterr()
    runtime = client.get("/api/v1/runtime")
    probe = client.get("/health/ready")
    events = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    status = "storage_unavailable" if unavailable else "ready"
    assert runtime.status_code == 200
    assert probe.status_code == (503 if unavailable else 200)
    assert runtime.json()["service_status"] == probe.json()["status"] == status
    assert runtime.json()["accepting_submissions"] is (not unavailable)
    assert calls == ["ping", "ping"]
    requests = [event for event in events if event["event"] == "http_request"]
    assert len(requests) == (2 if unavailable else 1)
    assert requests[0]["status"] == 200
    assert requests[0]["error_code"] == ("storage_unavailable" if unavailable else "none")
    if unavailable:
        assert requests[1]["status"] == 503
        assert requests[1]["error_code"] == "storage_unavailable"
    assert "PRIVATE" not in json.dumps(events) + runtime.text + probe.text


@pytest.mark.component
@pytest.mark.recovery
def test_shutdown_runtime_override_does_not_change_probe_or_touch_storage(factory, monkeypatch):
    client = factory()
    client.portal.call(client.app.state.coordinator.close)

    def forbidden_ping():
        pytest.fail("Readiness must not probe storage after admission closes")

    monkeypatch.setattr(client.app.state.store, "ping", forbidden_ping)
    runtime = client.get("/api/v1/runtime")
    probe = client.get("/health/ready")
    assert runtime.status_code == 200
    assert runtime.json()["service_status"] == "shutting_down"
    assert runtime.json()["accepting_submissions"] is False
    # The health endpoint preserves the last operational state during orderly shutdown.
    assert probe.status_code == 503
    assert probe.json() == {"status": "ready"}


@pytest.mark.component
def test_connecting_runtime_and_probe_do_not_check_storage(factory, monkeypatch):
    import threading

    from backend.tests.support import FakeReviewClient

    release = threading.Event()
    entered = threading.Event()

    class ConnectingClient(FakeReviewClient):
        def connect(self):
            entered.set()
            assert release.wait(5)

    client = factory(ConnectingClient(), ready=False)
    assert entered.wait(2)
    calls = []
    monkeypatch.setattr(client.app.state.store, "ping", lambda: calls.append("ping"))
    try:
        runtime = client.get("/api/v1/runtime")
        probe = client.get("/health/ready")
        assert runtime.status_code == 200
        assert runtime.json()["service_status"] == "inference_connecting"
        assert runtime.json()["accepting_submissions"] is False
        assert client.get("/health/live").status_code == 200
        assert probe.status_code == 503 and probe.json() == {"status": "inference_connecting"}
        assert calls == []
    finally:
        release.set()
