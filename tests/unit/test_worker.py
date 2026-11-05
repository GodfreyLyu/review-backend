import json
import threading
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from app.application.execution import InvalidDelivery
from app.worker.main import Worker

pytestmark = pytest.mark.unit


class Consumer:
    def __init__(self):
        self.commits = []
        self.paused = []

    def assignment(self):
        return ["partition-0"]

    def commit(self, **kwargs):
        self.commits.append(kwargs)
        return []

    def resume(self, parts):
        pass

    def pause(self, parts):
        self.paused = parts


def active(worker, durable):
    future = Future()
    future.set_result(durable)
    worker.active = dict(
        future=future,
        message="offset-5",
        generation=worker.generation,
        stop=threading.Event(),
        retry_at=float("inf"),
    )


def test_offset_only_commits_after_durable_result_or_handoff():
    kafka = Consumer()
    worker = Worker(None, SimpleNamespace(retry_base_seconds=1), kafka, executor=object())
    try:
        active(worker, False)
        worker._finish()
        assert kafka.commits == []
        active(worker, True)
        worker._finish()
        assert kafka.commits == [{"message": "offset-5", "asynchronous": False}]
    finally:
        worker.pool.shutdown()


def test_rebalance_stops_work_and_never_commits_old_assignment():
    kafka = Consumer()
    worker = Worker(None, SimpleNamespace(retry_base_seconds=1), kafka, executor=object())
    try:
        active(worker, True)
        stop = worker.active["stop"]
        worker.on_revoke(kafka, ["partition-0"])
        worker._finish()
        assert stop.is_set()
        assert kafka.commits == []
        assert worker.active is None
    finally:
        worker.pool.shutdown()


def test_semantically_invalid_delivery_is_quarantined_before_acknowledgement():
    data = dict(
        event_id=str(uuid4()),
        review_id=str(uuid4()),
        trace_id=str(uuid4()),
        attempt=999,
        schema_version=1,
        execution_version="review-v1",
        created_at=123.0,
    )
    payload = json.dumps(data).encode()
    message = SimpleNamespace(
        value=lambda: payload,
        key=lambda: data["review_id"].encode(),
        topic=lambda: "review.jobs.v1",
        partition=lambda: 0,
        offset=lambda: 5,
    )
    store, executor = Mock(), Mock()
    executor.handle.side_effect = InvalidDelivery("invalid_review_or_attempt")
    worker = Worker(store, SimpleNamespace(retry_base_seconds=1), Consumer(), executor)
    try:
        assert worker._process(message, threading.Event()) is True
        store.quarantine_message.assert_called_once_with(
            "review.jobs.v1", 0, 5, "invalid_review_or_attempt", payload
        )
        store.quarantine_message.side_effect = RuntimeError("database unavailable")
        with pytest.raises(RuntimeError):
            worker._process(message, threading.Event())
    finally:
        worker.pool.shutdown()
