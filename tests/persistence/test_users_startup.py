"""Startup retry behavior with controlled time, temporary storage and fault injection."""

import asyncio
import json
import logging
import threading
from unittest.mock import Mock

import pytest
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    NoCredentialsError,
    ParamValidationError,
    ReadTimeoutError,
    SSLError,
)

from app.logging import SafeFormatter
from app.persistence import users_startup
from app.persistence.storage import Store
from app.persistence.users import DynamoUsers
from backend.tests.support import FakeReviewClient, register, wait_ready

PRIVATE = "http://PRIVATE_KEY:PRIVATE_PASSWORD@private-address:8000"


def connection_error(kind=ConnectTimeoutError):
    return kind(endpoint_url=PRIVATE)


@pytest.fixture
def clock(monkeypatch):
    class Clock:
        now = 0.0
        waits = []

        async def wait(self, shutdown, delay):
            self.waits.append(delay)
            self.now += delay
            await asyncio.sleep(0)

    clock = Clock()
    monkeypatch.setattr(users_startup, "monotonic", lambda: clock.now)
    monkeypatch.setattr(users_startup, "wait_for_stop", clock.wait)
    return clock


@pytest.mark.unit
def test_first_check_success_without_wait(clock):
    ping = Mock()
    assert asyncio.run(users_startup.check_users_storage(ping, asyncio.Event())) is True
    ping.assert_called_once_with()
    assert clock.waits == []


@pytest.mark.unit
@pytest.mark.recovery
@pytest.mark.parametrize("kind", users_startup.TRANSIENT_CONNECTION_ERRORS)
def test_explicit_transport_failures_recover_serially(clock, kind):
    ping = Mock(side_effect=[connection_error(kind), connection_error(kind), None])
    assert asyncio.run(users_startup.check_users_storage(ping, asyncio.Event())) is True
    assert ping.call_count == 3
    assert clock.waits == [1, 2]


@pytest.mark.unit
@pytest.mark.recovery
def test_persistent_failure_exhausts_real_budget_without_real_wait(clock):
    started = []

    def ping():
        started.append(clock.now)
        raise connection_error()

    with pytest.raises(ConnectTimeoutError):
        asyncio.run(users_startup.check_users_storage(ping, asyncio.Event()))
    assert clock.waits[:5] == [1, 2, 4, 8, 10]
    assert max(clock.waits) == 10
    assert sum(clock.waits) == clock.now == 120
    assert started == [0, 1, 3, 7, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115]


@pytest.mark.unit
@pytest.mark.parametrize("late_success", [False, True])
def test_call_duration_counts_and_deadline_does_not_abandon_inflight_call(clock, late_success):
    starts, finishes = [], []

    def ping():
        starts.append(clock.now)
        clock.now += 8  # One SDK request: 3s connect + 5s read; no SDK retries.
        finishes.append(clock.now)
        if not late_success or clock.now < 120:
            raise connection_error(ReadTimeoutError)

    expected = users_startup.UsersStorageStartupTimeout if late_success else ReadTimeoutError
    with pytest.raises(expected):
        asyncio.run(users_startup.check_users_storage(ping, asyncio.Event()))
    assert all(start < 120 for start in starts)
    assert len(starts) == len(finishes)
    assert 120 <= clock.now < 128
    assert all(
        previous <= following for previous, following in zip(finishes, starts[1:], strict=False)
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "error",
    [
        ValueError("PRIVATE schema or configuration"),
        ParamValidationError(report="PRIVATE parameter"),
        NoCredentialsError(),
        SSLError(endpoint_url=PRIVATE, error="PRIVATE certificate"),
        RuntimeError("PRIVATE unknown error"),
    ]
    + [
        ClientError({"Error": {"Code": code, "Message": "PRIVATE"}}, "DescribeTable")
        for code in [
            "AccessDeniedException",
            "ResourceNotFoundException",
            "ValidationException",
            "UnrecognizedClientException",
            "InternalServerError",
        ]
    ],
)
def test_non_transport_errors_fail_immediately(clock, error):
    ping = Mock(side_effect=error)
    with pytest.raises(type(error)) as raised:
        asyncio.run(users_startup.check_users_storage(ping, asyncio.Event()))
    assert raised.value is error
    ping.assert_called_once_with()
    assert clock.waits == []


@pytest.mark.component
@pytest.mark.parametrize("recover", [True, False])
def test_coordinator_stays_responsive_then_continues_once_or_fails(
    factory, monkeypatch, clock, recover
):
    order = []
    waiting = threading.Event()
    resume = asyncio.Event()
    initialize, recover_queue = Store.initialize, Store.recover

    def init(store):
        order.append("initialize")
        initialize(store)

    def queue(store, *args):
        order.append("recover")
        return recover_queue(store, *args)

    def ping(_users):
        order.append("users")
        if not recover or order.count("users") < 3:
            raise connection_error()

    class RecordingClient(FakeReviewClient):
        def connect(self):
            order.append("connect")

    async def wait(shutdown, delay):
        waiting.set()
        await resume.wait()
        await clock.wait(shutdown, delay)

    monkeypatch.setattr(Store, "initialize", init)
    monkeypatch.setattr(Store, "recover", queue)
    monkeypatch.setattr(DynamoUsers, "ping", ping)
    monkeypatch.setattr(users_startup, "wait_for_stop", wait)
    client = factory(RecordingClient(), ready=False)
    try:
        assert waiting.wait(2)
        assert order == ["initialize", "users"]
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").json() == {"status": "initializing"}
        assert client.get("/health/ready").status_code == 503
        assert client.get("/api/v1/runtime").json()["accepting_submissions"] is False
        register(client)
        assert client.post("/api/v1/reviews", json={"source_code": "x = 1"}).status_code == 503
        client.portal.call(resume.set)
        if recover:
            wait_ready(client)
            assert order == ["initialize", "users", "users", "users", "recover", "connect"]
            assert client.get("/api/v1/runtime").json()["accepting_submissions"] is True
            assert client.post("/api/v1/reviews", json={"source_code": "x = 1"}).status_code == 202
        else:

            async def finished():
                await asyncio.wait_for(client.app.state.coordinator.task, 2)

            client.portal.call(finished)
            assert client.get("/health/ready").json() == {"status": "startup_or_storage_failure"}
            assert client.get("/health/live").status_code == 503
            assert order.count("initialize") == 1
            assert "recover" not in order and "connect" not in order
            assert clock.now == 120
    finally:
        client.portal.call(resume.set)


@pytest.mark.component
@pytest.mark.recovery
def test_shutdown_wakes_actual_backoff_without_more_checks(factory, monkeypatch):
    waiting = threading.Event()
    actual_wait = users_startup.wait_for_stop
    ping = Mock(side_effect=connection_error())
    review_client = FakeReviewClient()
    review_client.connect = Mock()
    recover = Mock()

    async def wait(shutdown, _delay):
        waiting.set()
        await actual_wait(shutdown, 10)  # Real wait must wake on close, not after ten seconds.

    monkeypatch.setattr(DynamoUsers, "ping", ping)
    monkeypatch.setattr(Store, "recover", recover)
    monkeypatch.setattr(users_startup, "wait_for_stop", wait)
    client = factory(review_client, ready=False)
    assert waiting.wait(2)

    async def close():
        await asyncio.wait_for(client.app.state.coordinator.close(), 1)

    client.portal.call(close)
    ping.assert_called_once_with()
    recover.assert_not_called()
    review_client.connect.assert_not_called()
    assert client.get("/health/ready").status_code == 503


@pytest.mark.component
@pytest.mark.recovery
@pytest.mark.parametrize("fail", [False, True])
def test_shutdown_drains_inflight_check_and_never_starts_next_stage(factory, monkeypatch, fail):
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()
    calls = []
    review_client = FakeReviewClient()
    review_client.connect = Mock()
    recover = Mock()

    def ping(_users):
        calls.append("ping")
        entered.set()
        try:
            assert release.wait(2)
            if fail:
                raise connection_error(ConnectionClosedError)
        finally:
            exited.set()

    monkeypatch.setattr(DynamoUsers, "ping", ping)
    monkeypatch.setattr(Store, "recover", recover)
    client = factory(review_client, ready=False)
    try:
        assert entered.wait(2)
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 503

        async def close():
            closing = asyncio.create_task(client.app.state.coordinator.close())
            await asyncio.sleep(0)
            assert not closing.done()
            release.set()
            await asyncio.wait_for(closing, 1)

        client.portal.call(close)
        assert exited.is_set() and calls == ["ping"]
        recover.assert_not_called()
        review_client.connect.assert_not_called()
        assert not client.app.state.coordinator.ready
    finally:
        release.set()


@pytest.mark.unit
@pytest.mark.recovery
def test_safe_retry_logs_include_attempt_wait_and_type_without_exception_text(clock, capsys):
    from app.config import Settings
    from app.logging import configure_logging

    configure_logging(
        Settings(
            _env_file=None,
            signing_secret="test-" * 10,
            dynamodb_endpoint_url="http://127.0.0.1:8001",
        )
    )
    ping = Mock(side_effect=[connection_error(), None])
    asyncio.run(users_startup.check_users_storage(ping, asyncio.Event()))
    output = capsys.readouterr().err
    assert "PRIVATE" not in output and "private-address" not in output
    events = [json.loads(line) for line in output.splitlines()]
    retry = next(event for event in events if event["event"] == "users_storage_retry_scheduled")
    assert retry["stage"] == "users_storage"
    assert retry["startup_attempt"] == 1
    assert retry["startup_wait_ms"] == 1000
    assert retry["exception_type"] == "ConnectTimeoutError"
    assert events[-1]["event"] == "users_storage_ready"
    assert events[-1]["startup_attempt"] == 2
    assert events[-1]["startup_elapsed_ms"] == 1000


@pytest.mark.unit
@pytest.mark.recovery
def test_formatter_drops_invalid_startup_retry_fields():
    record = logging.makeLogRecord(
        {
            "msg": "users_storage_retry_scheduled",
            "startup_attempt": "PRIVATE",
            "startup_wait_ms": -1,
            "startup_elapsed_ms": True,
        }
    )
    payload = json.loads(SafeFormatter().format(record))
    assert not {"startup_attempt", "startup_wait_ms", "startup_elapsed_ms"} & payload.keys()


@pytest.mark.component
@pytest.mark.recovery
@pytest.mark.parametrize(
    "error",
    [
        ValueError("PRIVATE schema"),
        ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "PRIVATE"}}, "DescribeTable"
        ),
    ],
)
def test_nonretryable_failure_enters_existing_failed_state(factory, monkeypatch, clock, error):
    ping = Mock(side_effect=error)
    review_client = FakeReviewClient()
    review_client.connect = Mock()
    recover = Mock()
    monkeypatch.setattr(DynamoUsers, "ping", ping)
    monkeypatch.setattr(Store, "recover", recover)
    client = factory(review_client, ready=False)

    async def finished():
        await asyncio.wait_for(client.app.state.coordinator.task, 1)

    client.portal.call(finished)
    assert client.get("/health/ready").json() == {"status": "startup_or_storage_failure"}
    assert client.get("/health/live").status_code == 503
    ping.assert_called_once_with()
    recover.assert_not_called()
    review_client.connect.assert_not_called()
    assert clock.waits == []


@pytest.mark.unit
@pytest.mark.recovery
def test_shutdown_before_first_check_starts_no_request(clock):
    ping = Mock()
    shutdown = asyncio.Event()
    shutdown.set()
    assert asyncio.run(users_startup.check_users_storage(ping, shutdown)) is False
    ping.assert_not_called()
    assert clock.waits == []
