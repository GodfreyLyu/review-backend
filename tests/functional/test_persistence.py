"""Real PostgreSQL admission, lease fencing and transaction recovery checks."""

import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.errors import AppError
from app.main import create_app
from app.persistence.users import PostgresUsers
from tests.support import register

pytestmark = pytest.mark.functional


def user(store, name=None):
    return PostgresUsers(store).create(name or str(uuid4()), "not-a-login-hash")["user_id"]


def admit(store, owner=None, key=None, **kwargs):
    return store.create_review(
        owner or user(store),
        key or str(uuid4()),
        "python",
        "value = 1",
        kwargs.pop("capacity", 8),
        **kwargs,
    )


def test_concurrent_replicas_cannot_exceed_capacity_or_per_user_active(store):
    users = [user(store) for _ in range(12)]

    def submit(owner):
        try:
            return admit(store, owner, capacity=3).created
        except AppError as exc:
            assert exc.code == "queue_full"
            return False

    with ThreadPoolExecutor(max_workers=12) as pool:
        assert sum(pool.map(submit, users)) == 3
    assert store.queue_depth() == 3
    with store.connection(write=True) as db:
        db.execute("UPDATE reviews SET status='failed'")
    owner = user(store)

    def same_user(_):
        try:
            return admit(store, owner).created
        except AppError as exc:
            assert exc.code == "review_active"
            return False

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(same_user, range(8))) == 1


def test_idempotence_precedes_capacity_quota_and_writes_one_outbox(store):
    owner, key = user(store), str(uuid4())
    first = admit(store, owner, key, capacity=1, submission_limit=1)
    assert (
        admit(store, owner, key, capacity=1, submission_limit=1).review["review_id"]
        == first.review["review_id"]
    )
    with pytest.raises(AppError, match="different input"):
        store.create_review(owner, key, "python", "different", 1, submission_limit=1)
    with store.connection() as db:
        assert db.execute("SELECT count(*) AS n FROM outbox").fetchone()["n"] == 1
        event = db.execute("SELECT payload FROM outbox").fetchone()["payload"]
        assert event["attempt"] == 1 and event["schema_version"] == 1
        assert "source_code" not in event and "review_result" not in event


def test_outbox_claim_expiry_recovers_publish_before_mark_crash(store):
    admit(store)
    first = store.claim_outbox("relay-a", 10)
    assert len(first) == 1
    assert not store.claim_outbox("relay-b", 10)
    with store.connection(write=True) as db:
        db.execute("UPDATE outbox SET claim_until=extract(epoch FROM clock_timestamp())-1")
    second = store.claim_outbox("relay-b", 10)
    assert second[0]["event_id"] == first[0]["event_id"]
    assert not store.mark_outbox_sent(first[0]["event_id"], "relay-a")
    assert store.mark_outbox_sent(second[0]["event_id"], "relay-b")
    assert not store.claim_outbox("relay-c", 10)


def test_expired_lease_fences_late_result_and_keeps_uncertain_slot(store):
    initial = admit(store)
    review_id = initial.review["review_id"]
    old = store.claim_review(review_id, "old", 10, 1)
    assert store.record_rpc(review_id, old["fence"], "instance-a", "request-a", "summary")
    with store.connection(write=True) as db:
        db.execute(
            "UPDATE reviews SET lease_until=extract(epoch FROM clock_timestamp())-1 "
            "WHERE review_id=%s",
            (review_id,),
        )
    assert store.recover_due(delay=0, max_start_seconds=0) >= 1
    recovered = store.claim_review(review_id, "new", 10, 1)
    assert recovered["attempt"] == 1 and recovered["fence"] > old["fence"]
    assert recovered["rpc_request_id"] == "request-a"
    assert not store.save_section(review_id, old["fence"], "summary", "late.", "model", "revision")
    assert not store.finish_review(review_id, recovered["fence"], result="unsafe")
    assert store.defer_review(review_id, recovered["fence"], "unknown", 0, 3, uncertain=True)
    another = admit(store)
    assert store.claim_review(another.review["review_id"], "other", 10, 1) is None
    with store.connection() as db:
        row = db.execute("SELECT * FROM reviews WHERE review_id=%s", (review_id,)).fetchone()
        assert (
            row["execution_state"] == "recovery_required" and row["rpc_request_id"] == "request-a"
        )


def test_retry_is_persisted_delayed_and_bounded(store):
    initial = admit(store)
    review_id = initial.review["review_id"]
    job = store.claim_review(review_id, "one", 10, 1)
    assert store.defer_review(review_id, job["fence"], "inference_unavailable", 0.1, 2)
    assert store.delivery_disposition(review_id, 1) == "done"
    assert store.claim_review(review_id, "early", 10, 2) is None
    time.sleep(0.12)
    second = store.claim_review(review_id, "two", 10, 2)
    assert second["attempt"] == 2
    assert store.defer_review(review_id, second["fence"], "inference_unavailable", 0, 2)
    with store.connection() as db:
        assert (
            db.execute("SELECT status FROM reviews WHERE review_id=%s", (review_id,)).fetchone()[
                "status"
            ]
            == "failed"
        )
        assert db.execute("SELECT 1 FROM outbox WHERE topic='review.jobs.dlq.v1'").fetchone()


def test_api_history_auth_csrf_isolation_and_no_worker(settings, store):
    app = create_app(settings, store=store)
    assert not hasattr(app.state, "coordinator") and not hasattr(app.state, "review_client")
    with TestClient(app) as client:
        client.headers["Origin"] = settings.allowed_origin
        assert client.get("/health/ready").status_code == 200
        assert client.post("/api/v1/reviews", json={"source_code": "value=1"}).status_code == 401
        registration = register(client)
        payload = {"source_code": "value=1", "client_request_id": str(uuid4())}
        client.headers["X-CSRF-Token"] = "wrong"
        assert client.post("/api/v1/reviews", json=payload).status_code == 403
        client.headers["X-CSRF-Token"] = registration.json()["csrf_token"]
        accepted = client.post("/api/v1/reviews", json=payload)
        assert accepted.status_code == 202
        review_id = accepted.json()["review_id"]
        assert client.post("/api/v1/reviews", json=payload).json() == accepted.json()
        detail = client.get(f"/api/v1/reviews/{review_id}").json()
        assert detail["status"] == "queued" and detail["execution_state"] == "queued"
        assert "rpc_request_id" not in detail and "user_id" not in detail
        client.post("/api/v1/auth/logout", json={})
        register(client, "bob")
        assert client.get(f"/api/v1/reviews/{review_id}").status_code == 404
        assert client.get("/api/v1/reviews").json()["items"] == []


def test_queued_deadline_expires_without_model_execution(store):
    first = admit(store, deadline_seconds=0.01)
    time.sleep(0.02)
    assert store.recover_due(max_start_seconds=1000) == 1
    with store.connection() as db:
        row = db.execute(
            "SELECT * FROM reviews WHERE review_id=%s", (first.review["review_id"],)
        ).fetchone()
        assert row["status"] == "failed" and row["error_code"] == "task_expired"


def test_slot_contention_handoff_allows_recovery_event_polling(store):
    a = admit(store).review["review_id"]
    running = store.claim_review(a, "a", 10, 1)
    assert store.record_rpc(a, running["fence"], "instance-a", "request-a", "summary")
    assert store.defer_review(a, running["fence"], "unknown", 5, 3, uncertain=True)
    b = admit(store).review["review_id"]
    assert store.claim_review(b, "b", 10, 1) is None
    # The B Kafka offset can be acknowledged after a new, delayed DB handoff.
    # It therefore cannot block polling the A recovery record in the same partition.
    assert store.delivery_disposition(b, 1) == "done"
    with store.connection() as db:
        row = db.execute("SELECT * FROM reviews WHERE review_id=%s", (b,)).fetchone()
        assert row["attempt"] == 0 and row["execution_state"] == "retry_wait"
        event = db.execute(
            "SELECT * FROM outbox WHERE review_id=%s AND payload->>'reason'='slot_busy'", (b,)
        ).fetchone()
        assert event["payload"]["attempt"] == 1
        assert event["available_at"] > time.time()


def test_recovery_exhaustion_is_blocked_retains_slot_and_operator_reconciles(store):
    review_id = admit(store).review["review_id"]
    job = store.claim_review(review_id, "one", 10, 1)
    assert store.record_rpc(review_id, job["fence"], "instance", "request", "summary")
    assert store.defer_review(
        review_id, job["fence"], "unknown", 0, 3, uncertain=True, max_recovery_attempts=1
    )
    assert store.claim_review(review_id, "duplicate", 10, 1) is None
    assert store.delivery_disposition(review_id, 1) == "done"
    assert store.recover_due(delay=0, max_start_seconds=0) == 0
    with store.connection() as db:
        row = db.execute("SELECT * FROM reviews WHERE review_id=%s", (review_id,)).fetchone()
        assert row["execution_state"] == "recovery_blocked"
        assert row["rpc_request_id"] == "request"
        assert (
            db.execute("SELECT review_id FROM inference_slots").fetchone()["review_id"] == review_id
        )
    assert store.retry_recovery(review_id)
    recovered = store.claim_review(review_id, "operator", 10, 1)
    assert recovered["rpc_request_id"] == "request" and recovered["attempt"] == 1


def test_whole_review_execution_deadline_survives_lease_recovery(store):
    review_id = admit(store).review["review_id"]
    first = store.claim_review(review_id, "first", 0.01, 1, execution_timeout=11)
    assert 10 < first["execution_deadline_at"] - first["started_at"] <= 11
    time.sleep(0.02)
    store.recover_due(delay=0, max_start_seconds=0)
    second = store.claim_review(review_id, "second", 10, 1, execution_timeout=500)
    assert second["started_at"] == first["started_at"]
    assert second["execution_deadline_at"] == first["execution_deadline_at"]


def test_sessions_shared_across_api_replicas_and_logout_revokes_them(settings, store):
    first = TestClient(create_app(settings, store=store))
    second = TestClient(create_app(settings, store=store))
    with first, second:
        first.headers["Origin"] = settings.allowed_origin
        second.headers["Origin"] = settings.allowed_origin
        registered = register(first)
        second.cookies.update(first.cookies)
        assert second.get("/api/v1/auth/me").json() == registered.json()
        with store.connection() as db:
            row = db.execute("SELECT * FROM sessions").fetchone()
            assert row["token_hash"] not in first.cookies.values()
            assert len(row["token_hash"]) == 64
            account = db.execute("SELECT * FROM users").fetchone()
            assert account["password_hash"].startswith("$argon2id$")
        assert first.post("/api/v1/auth/logout", json={}).status_code == 204
        assert second.get("/api/v1/auth/me").status_code == 401
        login = first.post(
            "/api/v1/auth/login", json={"login_id": "ALICE", "password": "correct-horse-battery"}
        )
        assert login.status_code == 200
        assert login.json()["csrf_token"] != registered.json()["csrf_token"]
        first.headers["X-CSRF-Token"] = login.json()["csrf_token"]
        first.headers["Origin"] = "https://untrusted.invalid"
        assert first.post("/api/v1/reviews", json={"source_code": "x=1"}).status_code == 403


def test_rate_quota_is_shared_by_database_connections(store):
    from app.persistence.storage import Store

    second = Store(store.database_url)
    store.check_rate_limit("auth:shared", 1)
    with pytest.raises(AppError) as error:
        second.check_rate_limit("auth:shared", 1)
    assert error.value.code == "rate_limited"


def test_repeated_crash_recovery_is_bounded_even_before_worker_can_defer(store):
    review_id = admit(store).review["review_id"]
    job = store.claim_review(review_id, "crashed", 10, 1)
    assert store.record_rpc(review_id, job["fence"], "instance", "request", "summary")
    with store.connection(write=True) as db:
        db.execute(
            "UPDATE reviews SET lease_until=extract(epoch FROM clock_timestamp())-1 "
            "WHERE review_id=%s",
            (review_id,),
        )
    assert store.recover_due(delay=0, max_start_seconds=0, max_recovery_attempts=1) == 1
    assert store.claim_review(review_id, "replay", 10, 1) is None
    with store.connection() as db:
        row = db.execute("SELECT * FROM reviews WHERE review_id=%s", (review_id,)).fetchone()
        assert row["execution_state"] == "recovery_blocked"
        assert row["rpc_request_id"] == "request"


def test_future_attempt_and_missing_review_are_invalid_deliveries(store):
    review_id = admit(store).review["review_id"]
    assert store.claim_review(review_id, "future", 10, 999) is None
    assert store.delivery_disposition(review_id, 999) == "invalid"
    assert store.delivery_disposition(str(uuid4()), 1) == "invalid"
    # A poison future attempt must not change the legitimate task's schedule/state.
    with store.connection() as db:
        row = db.execute("SELECT * FROM reviews WHERE review_id=%s", (review_id,)).fetchone()
        assert row["attempt"] == 0 and row["execution_state"] == "queued"
