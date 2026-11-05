import copy
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from llm_contract.generated.ttypes import (
    GenerationOptions,
    GenerationResult,
    GenerationSnapshot,
    Limits,
    LlmError,
    ModelInfo,
    TokenUsage,
)
from llm_contract.generated.ttypes import GenerationState as State

from app.application.execution import InvalidDelivery, ReviewExecutor
from app.messaging.events import ReviewJob

pytestmark = pytest.mark.unit

BODIES = [
    "The average function computes a mean from values.",
    "Empty values causes division by zero.",
    "Guard average against an empty list.",
]


def settings():
    return SimpleNamespace(
        worker_lease_seconds=30,
        retry_max_seconds=20,
        retry_base_seconds=0.01,
        max_retries=2,
        max_recovery_attempts=8,
        inference_timeout_seconds=30,
        model_max_output_tokens=384,
        model_temperature=None,
        model_top_p=None,
        thrift_recovery_seconds=0.01,
        worker_poll_seconds=0.001,
        environment="test",
    )


class Store:
    def __init__(self):
        self.job = dict(
            review_id=str(uuid4()),
            source_code="def average(values): return sum(values)/len(values)",
            language="python",
            attempt=1,
            fence=1,
            deadline_at=time.time() + 30,
            sections={},
            rpc_instance_id=None,
            rpc_request_id=None,
            rpc_section=None,
            model_id=None,
            model_revision=None,
        )
        self.claimed = True
        self.leased = True
        self.finished = None
        self.deferred = None
        self.actions = []

    def claim_review(self, *args, **kwargs):
        return copy.deepcopy(self.job) if self.claimed else None

    def delivery_disposition(self, *args):
        return "done" if self.finished else "pending"

    def heartbeat(self, *args):
        return self.leased

    def record_rpc(self, review_id, fence, instance_id, request_id, section, **identity):
        self.actions.append("intent")
        self.job.update(rpc_instance_id=instance_id, rpc_request_id=request_id, rpc_section=section)
        self.job.update(model_id=identity.get("model_id"), model_revision=identity.get("revision"))
        return self.leased

    def save_section(self, review_id, fence, section, body, model_id, revision):
        self.actions.append("section")
        self.job["sections"][section] = body
        self.job.update(model_id=model_id, model_revision=revision)
        self.mark_rpc_terminal(review_id, fence, "succeeded")
        return self.leased

    def mark_rpc_terminal(self, *args):
        self.job.update(rpc_instance_id=None, rpc_request_id=None, rpc_section=None)
        return self.leased

    def finish_review(self, *args, **kwargs):
        self.finished = kwargs
        return self.leased

    def defer_review(self, *args, **kwargs):
        self.deferred = (args, kwargs)
        return self.leased


class Remote:
    def __init__(self, store):
        self.store = store
        self.info = ModelInfo(
            str(uuid4()),
            "model",
            "sha256:" + "a" * 64,
            GenerationOptions(),
            Limits(1, 1, 100, 600000, 300000, 64000, 2048, 512),
        )
        self.started = []
        self.snapshots = {}
        self.lost = False
        self.unknown = False

    def connect(self):
        return self.info

    def start(self, request):
        assert self.store.actions[-1] == "intent"
        self.started.append(request)
        result = GenerationResult(
            BODIES[(len(self.started) - 1) % 3],
            self.info.model_id,
            self.info.model_revision,
            TokenUsage(10, 10),
            "stop",
        )
        snapshot = GenerationSnapshot(
            request.request_id, request.instance_id, State.SUCCEEDED, result
        )
        self.snapshots[request.request_id] = snapshot
        if self.lost:
            self.lost = False
            raise OSError("lost reply")
        return snapshot

    def get(self, instance_id, request_id):
        if self.unknown:
            raise LlmError("not_found", "private", False)
        return self.snapshots[request_id]

    cancel = get


def event(store):
    return ReviewJob(str(uuid4()), store.job["review_id"], 1, str(uuid4()), time.time())


def test_durable_intent_precedes_rpc_and_lost_start_is_read_without_resending():
    store = Store()
    remote = Remote(store)
    remote.lost = True
    assert ReviewExecutor(store, settings(), remote).handle(event(store), threading.Event())
    assert len(remote.started) == 3
    assert store.finished["result"].startswith("## Summary")
    assert list(store.job["sections"]) == ["summary", "findings", "suggestions"]


def test_unknown_remote_state_retains_intent_and_hands_off_recovery_without_start():
    store = Store()
    remote = Remote(store)
    store.job.update(
        rpc_instance_id=remote.info.instance_id, rpc_request_id=str(uuid4()), rpc_section="summary"
    )
    remote.unknown = True
    assert ReviewExecutor(store, settings(), remote).handle(event(store), threading.Event())
    assert not remote.started
    assert store.deferred[1] == {"uncertain": True, "max_recovery_attempts": 8}
    assert store.job["rpc_request_id"] is not None
    assert store.finished is None


def test_expired_fence_never_starts_or_finishes_generation():
    store = Store()
    remote = Remote(store)
    store.leased = False
    assert not ReviewExecutor(store, settings(), remote).handle(event(store), threading.Event())
    assert not remote.started and store.finished is None


def test_completed_duplicate_is_acknowledgeable_but_other_live_lease_is_not():
    store = Store()
    store.claimed = False
    executor = ReviewExecutor(store, settings(), Remote(store))
    assert not executor.handle(event(store), threading.Event())
    store.finished = {"result": "saved"}
    assert executor.handle(event(store), threading.Event())


def test_unknown_review_or_future_attempt_signals_durable_quarantine():
    store = Store()
    store.claimed = False
    store.delivery_disposition = lambda *_: "invalid"
    with pytest.raises(InvalidDelivery, match="invalid_review_or_attempt"):
        ReviewExecutor(store, settings(), Remote(store)).handle(event(store), threading.Event())


def test_recovered_successful_section_is_not_generated_again():
    store = Store()
    remote = Remote(store)
    request_id = str(uuid4())
    store.job.update(
        rpc_instance_id=remote.info.instance_id, rpc_request_id=request_id, rpc_section="summary"
    )
    remote.snapshots[request_id] = GenerationSnapshot(
        request_id,
        remote.info.instance_id,
        State.SUCCEEDED,
        GenerationResult(
            BODIES[0], remote.info.model_id, remote.info.model_revision, TokenUsage(10, 10), "stop"
        ),
    )
    assert ReviewExecutor(store, settings(), remote).handle(event(store), threading.Event())
    assert len(remote.started) == 2
    assert store.finished is not None


def test_first_section_wrong_model_revision_is_rejected_before_more_generation():
    store = Store()
    remote = Remote(store)
    original = remote.start

    def wrong_revision(request):
        snapshot = original(request)
        snapshot.result.model_revision = "sha256:" + "b" * 64
        return snapshot

    remote.start = wrong_revision
    assert ReviewExecutor(store, settings(), remote).handle(event(store), threading.Event())
    assert len(remote.started) == 1
    assert store.finished["error_code"] == "ollama_model_mismatch"
    assert store.job["rpc_request_id"] is None


def test_unsent_intent_after_shutdown_is_cleared_without_starting_gpu():
    store = Store()
    remote = Remote(store)
    stop = threading.Event()
    original = store.record_rpc

    def stopping(*args, **kwargs):
        result = original(*args, **kwargs)
        stop.set()
        return result

    store.record_rpc = stopping
    assert ReviewExecutor(store, settings(), remote).handle(event(store), stop)
    assert remote.started == []
    assert store.deferred[0][2] == "interrupted"
    assert store.job["rpc_request_id"] is None
