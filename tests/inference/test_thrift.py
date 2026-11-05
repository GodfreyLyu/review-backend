"""Backend-owned prompts, validation and uncertain-execution recovery."""

import copy
import threading
import time
import uuid

import pytest
from llm_contract.generated.ttypes import (
    GenerationOptions,
    GenerationResult,
    GenerationSnapshot,
    Health,
    Limits,
    LlmError,
    ModelInfo,
    TokenUsage,
)
from llm_contract.generated.ttypes import (
    GenerationState as State,
)
from pydantic import ValidationError
from thrift.Thrift import TApplicationException
from thrift.transport.TTransport import TTransportException

from app.errors import AppError, InferenceUnavailable, InferenceUncertain
from app.inference.rpc.client import ThriftReviewClient
from app.inference.rpc.transport import Connection
from app.inference.settings import InferenceSettings
from backend.tests.support import register, wait_review

SOURCE = "def average(values):\n    return sum(values) / len(values)\n"
BODIES = [
    "The `average` function computes a mean from `values`.",
    "Empty `values` causes division by zero.",
    "Guard `average` against an empty list.",
]


class Remote:
    def __init__(self):
        self.info = ModelInfo(
            str(uuid.uuid4()),
            "qwen3:1.7b",
            "sha256:" + "a" * 64,
            GenerationOptions(128, 0.7, 0.8),
            Limits(1, 8, 128, 600000, 300000, 64000, 2048, 512),
            "Q4_K_M",
            "gpu",
        )
        self.requests, self.results = [], {}
        self.bodies = BODIES[:]
        self.error = None
        self.lost = False
        self.state = State.SUCCEEDED

    def call(self, method, *args):
        if method == "getModelInfo":
            return copy.deepcopy(self.info)
        if method == "getHealth":
            return Health(True, False, 0, 0)
        if method == "startGeneration":
            request = args[0]
            if request.instance_id != self.info.instance_id:
                raise LlmError("instance_changed", "PRIVATE_SOURCE", False)
            self.requests.append(request)
            result = GenerationResult(
                self.bodies[(len(self.requests) - 1) % 3],
                self.info.model_id,
                self.info.model_revision,
                TokenUsage(100, 20),
                "stop",
                20,
                5,
            )
            snapshot = GenerationSnapshot(
                request.request_id, self.info.instance_id, self.state, result, error=self.error
            )
            self.results[request.request_id] = snapshot
            if self.lost:
                self.lost = False
                raise OSError("PRIVATE_SOURCE")
            return snapshot
        if args[0] != self.info.instance_id:
            raise LlmError("instance_changed", "PRIVATE_SOURCE", False)
        if args[1] not in self.results:
            raise LlmError("not_found", "PRIVATE_SOURCE", False)
        return self.results[args[1]]


@pytest.fixture
def remote(monkeypatch):
    review_client = ThriftReviewClient(
        InferenceSettings(_env_file=None, thrift_host="127.0.0.1", thrift_recovery_seconds=0.1)
    )
    remote = Remote()
    monkeypatch.setattr(review_client, "_call", remote.call)
    review_client.connect()
    return review_client, remote


@pytest.mark.component
@pytest.mark.contract
def test_backend_supplies_prompts_and_builds_the_review(remote):
    review_client, server = remote
    result = review_client.review(SOURCE, "python", threading.Event())
    assert result == "\n\n".join(
        f"## {title}\n{body}"
        for title, body in zip(("Summary", "Findings", "Suggestions"), BODIES, strict=True)
    )
    assert len(server.requests) == 3
    assert all(request.messages[0].role == "system" for request in server.requests)
    assert all(SOURCE in request.messages[1].content for request in server.requests)
    assert sum(r.options.max_output_tokens for r in server.requests) == 384
    assert all(r.options.temperature is None for r in server.requests)


@pytest.mark.component
@pytest.mark.recovery
def test_lost_acceptance_is_reconciled_by_id_without_resubmission(remote, caplog):
    review_client, server = remote
    server.lost = True
    assert review_client.review(SOURCE, "python", threading.Event()).startswith("## Summary")
    assert len(server.requests) == 3
    assert "PRIVATE" not in caplog.text


@pytest.mark.component
def test_fastapi_keeps_authentication_persistence_and_section_metrics(factory, remote):
    review_client, _ = remote
    client = factory(review_client)
    review_client.metrics = client.app.state.metrics
    register(client)
    response = client.post("/api/v1/reviews", json={"source_code": SOURCE, "language": "python"})
    result = wait_review(client, response.json()["review_id"])
    assert result["status"] == "completed" and result["model_revision"] == review_client.digest
    assert 'review_generated_tokens_total{section="summary"} 20.0' in client.get("/metrics").text


@pytest.mark.component
@pytest.mark.recovery
@pytest.mark.parametrize(
    "failure", ["unreachable", "connection_unavailable", "restarted", "expired"]
)
def test_unknown_execution_blocks_coordinator_without_repeating_generation(
    factory, remote, monkeypatch, failure
):
    review_client, server = remote
    original = server.call

    def lost(method, *args):
        if method in {"getGeneration", "cancelGeneration"} and failure == "unreachable":
            raise OSError("PRIVATE_SOURCE")
        if method in {"getGeneration", "cancelGeneration"} and failure == "connection_unavailable":
            raise InferenceUnavailable()
        try:
            return original(method, *args)
        except OSError:
            if failure == "restarted":
                server.info.instance_id = str(uuid.uuid4())
            elif failure == "expired":
                server.results.clear()
            raise

    server.lost = True
    monkeypatch.setattr(review_client, "_call", lost)
    client = factory(review_client)
    register(client)
    response = client.post("/api/v1/reviews", json={"source_code": SOURCE, "language": "python"})
    assert wait_review(client, response.json()["review_id"])["error_code"] == "inference_stuck"
    assert client.get("/health/live").status_code == 503
    assert client.get("/api/v1/runtime").json()["service_status"] == "inference_stuck"
    assert client.post("/api/v1/reviews", json={"source_code": "next"}).status_code == 503
    assert len(server.requests) == 1


@pytest.mark.component
def test_restart_before_admission_requires_a_new_connection_without_replaying_the_review(remote):
    review_client, server = remote
    server.info.instance_id = str(uuid.uuid4())
    with pytest.raises(InferenceUnavailable):
        review_client.review(SOURCE, "python", threading.Event())
    assert not server.requests
    review_client.connect()
    assert review_client.review(SOURCE, "python", threading.Event()).startswith("## Summary")
    assert len(server.requests) == 3


@pytest.mark.component
@pytest.mark.recovery
def test_connection_failure_before_start_is_reconnectable_without_reconciling_unknown_work(
    remote, monkeypatch
):
    review_client, server = remote

    def unavailable(method, *args):
        if method == "startGeneration":
            raise InferenceUnavailable()
        return server.call(method, *args)

    monkeypatch.setattr(review_client, "_call", unavailable)
    with pytest.raises(InferenceUnavailable):
        review_client.review(SOURCE, "python", threading.Event())
    assert not server.requests


@pytest.mark.component
@pytest.mark.recovery
def test_idle_health_check_refreshes_service_instance_and_result_provenance(
    factory, remote, monkeypatch
):
    review_client, server = remote
    monkeypatch.setattr("app.coordinator.INFERENCE_HEALTH_INTERVAL_SECONDS", 0.0)
    client = factory(review_client)
    replacement = copy.deepcopy(server.info)
    replacement.instance_id = str(uuid.uuid4())
    replacement.model_revision = "sha256:" + "b" * 64
    server.info = replacement
    for _ in range(200):
        if review_client.info.instance_id == replacement.instance_id:
            break
        time.sleep(0.01)
    assert review_client.info.instance_id == replacement.instance_id
    assert not server.requests
    register(client)
    response = client.post("/api/v1/reviews", json={"source_code": SOURCE, "language": "python"})
    result = wait_review(client, response.json()["review_id"])
    assert result["status"] == "completed"
    assert result["model_revision"] == replacement.model_revision
    assert all(request.instance_id == replacement.instance_id for request in server.requests)


@pytest.mark.component
def test_output_validation_remains_in_backend(remote):
    review_client, server = remote
    server.bodies = ["Unrelated prose."] * 3
    with pytest.raises(AppError) as error:
        review_client.review(SOURCE, "python", threading.Event())
    assert error.value.code == "invalid_model_response"


@pytest.mark.unit
@pytest.mark.parametrize(
    "host", ["example.com", "169.254.169.254", "http://localhost", "127.0.0.1:9091"]
)
def test_thrift_endpoint_is_local(host):
    with pytest.raises(ValidationError):
        InferenceSettings(_env_file=None, thrift_host=host)


@pytest.mark.component
@pytest.mark.recovery
def test_cancellation_reconciliation_is_bounded_without_claiming_remote_exit(remote, monkeypatch):
    review_client, server = remote
    stop = threading.Event()
    original = server.call
    server.state = State.RUNNING

    def running(method, *args):
        snapshot = original(method, *args)
        if method == "startGeneration":
            stop.set()
        return snapshot

    monkeypatch.setattr(review_client, "_call", running)
    with pytest.raises(InferenceUncertain):
        review_client.review(SOURCE, "python", stop)
    assert len(server.requests) == 1
    assert server.results[server.requests[0].request_id].state == State.RUNNING


@pytest.mark.component
@pytest.mark.security
@pytest.mark.parametrize(
    "remote_code,public_code",
    [
        ("input_limit", "token_limit"),
        ("invalid_response", "invalid_model_response"),
        ("model_missing", "ollama_model_missing"),
        ("ollama_unavailable", "inference_unavailable"),
    ],
)
def test_native_failures_keep_application_error_codes_without_remote_content(
    factory, remote, remote_code, public_code, monkeypatch, capsys
):
    review_client, server = remote
    original = server.call

    def fail_after_summary(method, *args):
        if method == "startGeneration" and len(server.requests) == 1:
            server.state = State.FAILED
            server.error = LlmError(remote_code, "PRIVATE_SOURCE_AND_GENERATED_CONTENT", False)
        return original(method, *args)

    monkeypatch.setattr(review_client, "_call", fail_after_summary)
    client = factory(review_client)
    review_client.metrics = client.app.state.metrics
    register(client)
    response = client.post("/api/v1/reviews", json={"source_code": SOURCE, "language": "python"})
    result = wait_review(client, response.json()["review_id"])
    assert result["status"] == "failed" and result["error_code"] == public_code
    assert len(server.requests) == 2
    metrics = client.get("/metrics").text
    assert 'review_generated_tokens_total{section="summary"} 20.0' in metrics
    assert 'review_generated_tokens_total{section="findings"}' not in metrics
    assert 'review_generated_tokens_total{section="suggestions"}' not in metrics
    assert "PRIVATE" not in str(result) + capsys.readouterr().err


@pytest.mark.unit
def test_transport_open_failure_is_unavailable_before_any_request_can_be_sent(monkeypatch):
    connection = Connection(InferenceSettings(_env_file=None, thrift_host="127.0.0.1"))
    closed = []

    def fail():
        raise TTransportException(message="PRIVATE_ENDPOINT")

    monkeypatch.setattr(connection.transport, "open", fail)
    monkeypatch.setattr(connection.transport, "close", lambda: closed.append(True))
    with pytest.raises(InferenceUnavailable) as error:
        with connection:
            pytest.fail("A failed connection must not reach the generated client")
    assert closed == [True]
    assert "PRIVATE" not in str(error.value)


@pytest.mark.component
def test_failed_handshake_forgets_previous_identity_and_can_connect_again(remote, monkeypatch):
    review_client, server = remote

    def unavailable(*_):
        raise OSError("PRIVATE_ENDPOINT")

    monkeypatch.setattr(review_client, "_call", unavailable)
    with pytest.raises(InferenceUnavailable):
        review_client.connect()
    assert review_client.info is None and review_client.digest is None
    monkeypatch.setattr(review_client, "_call", server.call)
    review_client.connect()
    assert review_client.info.instance_id == server.info.instance_id
    assert not server.requests


@pytest.mark.component
@pytest.mark.parametrize("health", [Health(False, False, 0, 0), Health(True, True, 0, 0)])
def test_handshake_requires_a_ready_service_that_is_not_draining(remote, monkeypatch, health):
    review_client, server = remote
    monkeypatch.setattr(
        review_client,
        "_call",
        lambda method: health if method == "getHealth" else server.call(method),
    )
    with pytest.raises(InferenceUnavailable):
        review_client.connect()
    assert review_client.info is None


@pytest.mark.component
def test_protocol_failure_is_permanent_and_does_not_expose_remote_messages(remote, monkeypatch):
    review_client, _ = remote
    calls = []

    def incompatible(method):
        calls.append(method)
        raise TApplicationException(TApplicationException.UNKNOWN_METHOD, "PRIVATE_PROTOCOL")

    monkeypatch.setattr(review_client, "_call", incompatible)
    with pytest.raises(AppError) as error:
        review_client.connect()
    assert not isinstance(error.value, InferenceUnavailable)
    assert calls == ["getModelInfo"]
    assert review_client.info is None
    assert "PRIVATE" not in str(error.value)


@pytest.mark.component
@pytest.mark.parametrize(
    "constraint",
    [{"expected_model_id": "another-model"}, {"expected_model_revision": "sha256:" + "b" * 64}],
)
def test_handshake_enforces_application_model_constraints(remote, constraint):
    review_client, _ = remote
    for name, value in constraint.items():
        setattr(review_client.settings, name, value)
    with pytest.raises(AppError) as error:
        review_client.connect()
    assert error.value.code == "ollama_model_mismatch"
    assert not isinstance(error.value, InferenceUnavailable)
    assert review_client.info is None


@pytest.mark.component
def test_closing_during_a_handshake_cannot_publish_a_new_connection(remote, monkeypatch):
    review_client, server = remote

    def close_during_health(method):
        result = server.call(method)
        if method == "getHealth":
            review_client.close()
        return result

    monkeypatch.setattr(review_client, "_call", close_during_health)
    with pytest.raises(InferenceUnavailable):
        review_client.connect()
    assert review_client.info is None
