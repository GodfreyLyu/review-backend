import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from app.errors import AppError
from app.persistence.storage import Store


@pytest.mark.integration
@pytest.mark.recovery
def test_persistent_recovery_and_bounded_retry(tmp_path):
    store = Store(tmp_path)
    store.initialize()
    first = store.create_review("alice", str(uuid4()), "auto", "source", 8)
    second = store.create_review("bob", str(uuid4()), "plain", "source", 8)
    assert store.claim()["review_id"] == first.review["review_id"]
    restarted = Store(tmp_path)
    restarted.initialize()
    restarted.recover(max_retries=1, capacity=8)
    assert restarted.get_review("alice", first.review["review_id"])["retry_count"] == 1
    assert restarted.get_review("bob", second.review["review_id"])["status"] == "queued"
    restarted.claim()
    restarted.recover(max_retries=1, capacity=8)
    assert restarted.get_review("alice", first.review["review_id"])["error_code"] == "interrupted"
    assert restarted.claim()["review_id"] == second.review["review_id"]


@pytest.mark.integration
def test_concurrent_idempotency_is_atomic(tmp_path):
    store = Store(tmp_path)
    store.initialize()
    request_id, barrier = str(uuid4()), threading.Barrier(8)

    def insert(_):
        barrier.wait()
        return store.create_review("alice", request_id, "auto", "x", 8).review["review_id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(insert, range(8)))
    assert len(set(results)) == 1 and len(store.history("alice", 20)["items"]) == 1


@pytest.mark.integration
@pytest.mark.recovery
def test_capacity_and_reduced_recovery_capacity(tmp_path):
    store = Store(tmp_path)
    store.initialize()
    store.create_review("alice", "1", "auto", "x", 2)
    store.create_review("bob", "1", "auto", "x", 2)
    with pytest.raises(AppError, match="queue is full"):
        store.create_review("charlie", "1", "auto", "x", 2)
    store.recover(1, 1)
    assert (
        store.get_review("bob", store.history("bob", 1)["items"][0]["review_id"])["status"]
        == "failed"
    )


@pytest.mark.integration
def test_queue_depth_is_a_content_free_aggregate(tmp_path):
    store = Store(tmp_path)
    store.initialize()
    first = store.create_review("alice", "1", "auto", "PRIVATE_SOURCE_ONE", 8)
    second = store.create_review("bob", "1", "auto", "PRIVATE_SOURCE_TWO", 8)

    assert first.created is True and first.queue_depth == 1
    assert second.created is True and second.queue_depth == 2
    assert store.queue_depth() == 2
    store.claim()
    assert store.queue_depth() == 1


@pytest.mark.integration
def test_history_cursor_is_stable_and_source_is_not_in_list(tmp_path):
    store = Store(tmp_path)
    store.initialize()
    for i in range(3):
        row = store.create_review("alice", str(i), "auto", "source", 8)
        store.claim()
        store.finish(row.review["review_id"], result="ok", model_id="model", revision="revision")
    first = store.history("alice", 2)
    second = store.history("alice", 2, first["next_cursor"])
    assert len(first["items"]) == 2 and len(second["items"]) == 1
    assert second["next_cursor"] is None and "source_code" not in first["items"][0]


@pytest.mark.integration
@pytest.mark.parametrize(
    "same_user,capacity,error", [(False, 2, "queue_full"), (True, 8, "review_active")]
)
def test_concurrent_admission_preserves_capacity_and_one_active_job(
    tmp_path, same_user, capacity, error
):
    store = Store(tmp_path)
    store.initialize()
    barrier = threading.Barrier(8)

    def insert(index):
        barrier.wait()
        try:
            return store.create_review(
                "alice" if same_user else str(index), str(index), "auto", "source", capacity
            )
        except AppError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(insert, range(8)))
    admitted = [result for result in results if not isinstance(result, str)]
    rejected = [result for result in results if isinstance(result, str)]
    assert len(admitted) == (1 if same_user else capacity)
    assert rejected == [error] * (8 - len(admitted))
    assert store.queue_depth() == len(admitted)


@pytest.mark.integration
@pytest.mark.recovery
def test_idempotent_replay_precedes_capacity_and_active_job_checks(tmp_path):
    store = Store(tmp_path)
    store.initialize()
    first = store.create_review("alice", "key", "auto", "source", 1)
    replay = store.create_review("alice", "key", "auto", "source", 1)
    assert replay.review == first.review
    assert replay.created is False and replay.queue_depth is None
    assert first.created is True and first.queue_depth == 1
    with pytest.raises(AppError) as failure:
        store.create_review("alice", "key", "auto", "changed", 1)
    assert failure.value.code == "idempotency_conflict"
    assert store.queue_depth() == 1


@pytest.mark.integration
def test_failed_insert_rolls_back_without_consuming_request_key_or_capacity(tmp_path):
    import sqlite3

    store = Store(tmp_path)
    store.initialize()
    with store.connection(write=True) as db:
        db.execute(
            "CREATE TRIGGER reject_insert BEFORE INSERT ON reviews "
            "BEGIN SELECT RAISE(ABORT, 'fixture failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError):
        store.create_review("alice", "key", "auto", "source", 1)
    assert store.queue_depth() == 0
    assert store.history("alice", 20)["items"] == []
    with store.connection(write=True) as db:
        db.execute("DROP TRIGGER reject_insert")
    result = store.create_review("alice", "key", "auto", "source", 1)
    assert result.created and store.queue_depth() == 1
