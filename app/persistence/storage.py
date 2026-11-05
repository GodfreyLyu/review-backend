"""PostgreSQL transactions for admission, durable handoff, leases and fencing.

All execution mutators check both fence and an unexpired database lease. The single
inference slot is released only after remote execution has a confirmed terminal state.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.domain import CreateReviewResult
from app.errors import AppError

ACTIVE = ("queued", "running")


class Store:
    def __init__(
        self,
        database_url: str,
        *,
        topic: str = "review.jobs.v1",
        dlq_topic: str = "review.jobs.dlq.v1",
    ) -> None:
        self.database_url = database_url
        self.topic = topic
        self.dlq_topic = dlq_topic

    @contextmanager
    def connection(self, write: bool = False) -> Iterator[psycopg.Connection]:
        # Every operation has its own short transaction; never hold DB locks over RPC/Kafka.
        with psycopg.connect(self.database_url, row_factory=dict_row, connect_timeout=3) as db:
            db.execute("SET LOCAL statement_timeout = '5s'")
            db.execute("SET LOCAL lock_timeout = '3s'")
            yield db

    @staticmethod
    def _now(db) -> float:
        return db.execute("SELECT extract(epoch FROM clock_timestamp())::float8 AS now").fetchone()[
            "now"
        ]

    def initialize(self) -> None:
        """Explicit versioned, checksum-verified migration; never called by API startup."""
        with self.connection(write=True) as db:
            db.execute("SELECT pg_advisory_xact_lock(881032419)")
            db.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version integer PRIMARY KEY, checksum text NOT NULL, installed_at "
                "timestamptz NOT NULL DEFAULT now())"
            )
            installed = {
                r["version"]: r["checksum"] for r in db.execute("SELECT * FROM schema_migrations")
            }
            paths = sorted((Path(__file__).parent / "migrations").glob("*.sql"))
            expected = {int(p.name.split("_")[0]) for p in paths}
            if set(installed) - expected:
                raise RuntimeError("Database schema is newer than this application.")
            for path in paths:
                version = int(path.name.split("_")[0])
                checksum = hashlib.sha256(path.read_bytes()).hexdigest()
                if version in installed:
                    if installed[version] != checksum:
                        raise RuntimeError("Applied database migration checksum mismatch.")
                    continue
                db.execute(path.read_text())
                db.execute(
                    "INSERT INTO schema_migrations(version,checksum) VALUES (%s,%s)",
                    (version, checksum),
                )

    migrate = initialize

    def ping(self) -> None:
        with self.connection() as db:
            versions = [
                r["version"]
                for r in db.execute("SELECT version FROM schema_migrations ORDER BY version")
            ]
            expected = [
                int(p.name.split("_")[0])
                for p in sorted((Path(__file__).parent / "migrations").glob("*.sql"))
            ]
            if versions != expected:
                raise RuntimeError("Database migrations are required.")
            db.execute("SELECT 1 FROM reviews LIMIT 1")

    @staticmethod
    def digest(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def session_create(self, token: str, user: dict, csrf_token: str, expires_at: float) -> None:
        with self.connection(write=True) as db:
            db.execute("DELETE FROM sessions WHERE expires_at <= %s", (self._now(db),))
            db.execute(
                "INSERT INTO sessions VALUES (%s,%s,%s,%s,%s)",
                (self.digest(token), user["user_id"], user["login_id"], csrf_token, expires_at),
            )

    def session_get(self, token: str) -> dict | None:
        with self.connection() as db:
            return db.execute(
                "SELECT * FROM sessions WHERE token_hash=%s AND expires_at>%s",
                (self.digest(token), self._now(db)),
            ).fetchone()

    def session_delete(self, token: str) -> None:
        with self.connection(write=True) as db:
            db.execute("DELETE FROM sessions WHERE token_hash=%s", (self.digest(token),))

    @staticmethod
    def _rate_limit(db, scope: str, limit: int, now: float, window: float = 60) -> None:
        bucket = int(now // window)
        row = db.execute(
            "INSERT INTO rate_limits(scope,bucket,count) VALUES (%s,%s,1) "
            "ON CONFLICT(scope,bucket) DO UPDATE SET count=rate_limits.count+1 "
            "WHERE rate_limits.count < %s RETURNING count",
            (scope, bucket, limit),
        ).fetchone()
        if not row:
            raise AppError("rate_limited", "Too many requests. Try again in one minute.", 429)
        # Bounded retention; quotas are shared by all API processes.
        db.execute("DELETE FROM rate_limits WHERE bucket < %s", (bucket - 2,))

    def check_rate_limit(self, key: str, limit: int, window: float = 60) -> None:
        with self.connection(write=True) as db:
            self._rate_limit(db, key, limit, self._now(db), window)

    def _enqueue(
        self,
        db,
        row: dict,
        now: float,
        attempt: int,
        reason: str,
        available_at: float | None = None,
    ) -> str:
        event_id = str(uuid4())
        payload = {
            "event_id": event_id,
            "schema_version": 1,
            "review_id": row["review_id"],
            "attempt": attempt,
            "execution_version": row["execution_version"],
            "trace_id": row["trace_id"],
            "created_at": now,
            "reason": reason,
        }
        db.execute(
            "INSERT INTO outbox(event_id,review_id,topic,payload,available_at,created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (
                event_id,
                row["review_id"],
                row["job_topic"],
                Jsonb(payload),
                available_at if available_at is not None else now,
                now,
            ),
        )
        db.execute(
            "UPDATE reviews SET last_enqueued_at=%s WHERE review_id=%s", (now, row["review_id"])
        )
        return event_id

    def create_review(
        self,
        user_id: str,
        request_id: str,
        language: str,
        source: str,
        capacity: int,
        *,
        trace_id: str | None = None,
        deadline_seconds: float = 600,
        max_active_per_user: int = 1,
        submission_limit: int = 6,
    ) -> CreateReviewResult:
        with self.connection(write=True) as db:
            # Serializes all admission decisions across API replicas, including quota and counts.
            db.execute("SELECT id FROM admission_lock WHERE id=1 FOR UPDATE")
            existing = db.execute(
                "SELECT * FROM reviews WHERE user_id=%s AND client_request_id=%s",
                (user_id, request_id),
            ).fetchone()
            if existing:
                if existing["source_code"] != source or existing["language"] != language:
                    raise AppError(
                        "idempotency_conflict", "This request key has different input.", 409
                    )
                return CreateReviewResult(review=existing, created=False)
            now = self._now(db)
            count = db.execute(
                "SELECT count(*) AS n FROM reviews WHERE status IN ('queued','running')"
            ).fetchone()["n"]
            if count >= capacity:
                raise AppError("queue_full", "The review queue is full. Try again later.", 429)
            active = db.execute(
                "SELECT count(*) AS n FROM reviews WHERE user_id=%s AND status IN "
                "('queued','running')",
                (user_id,),
            ).fetchone()["n"]
            if active >= max_active_per_user:
                raise AppError("review_active", "A review is already active for your account.", 409)
            self._rate_limit(db, "submit:" + user_id, submission_limit, now)
            review_id = str(uuid4())
            row = db.execute(
                "INSERT INTO reviews(review_id,user_id,client_request_id,source_code,language,"
                "available_at,deadline_at,created_at,updated_at,last_enqueued_at,trace_"
                "id,job_topic) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (
                    review_id,
                    user_id,
                    request_id,
                    source,
                    language,
                    now,
                    now + deadline_seconds,
                    now,
                    now,
                    now,
                    trace_id or str(uuid4()),
                    self.topic,
                ),
            ).fetchone()
            self._enqueue(db, row, now, 1, "submitted")
            return CreateReviewResult(review=row, created=True, queue_depth=count + 1)

    def queue_depth(self) -> int:
        return self.queue_snapshot()[0]

    def queue_snapshot(self) -> tuple[int, float | None]:
        with self.connection() as db:
            row = db.execute(
                "SELECT count(*) AS n,min(created_at) AS oldest FROM reviews WHERE status='queued'"
            ).fetchone()
            return row["n"], row["oldest"]

    def get_review(self, user_id: str, review_id: str) -> dict:
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM reviews WHERE user_id=%s AND review_id=%s", (user_id, review_id)
            ).fetchone()
            if not row:
                raise AppError("review_not_found", "Review not found.", 404)
            return row

    def history(self, user_id: str, limit: int, before: str | None = None) -> dict:
        with self.connection() as db:
            params: list[Any] = [user_id]
            cursor_filter = ""
            if before:
                cursor = db.execute(
                    "SELECT created_at FROM reviews WHERE user_id=%s AND review_id=%s",
                    (user_id, before),
                ).fetchone()
                if not cursor:
                    raise AppError("invalid_cursor", "Invalid history cursor.", 400)
                cursor_filter = " AND (created_at,review_id) < (%s,%s)"
                params.extend((cursor["created_at"], before))
            params.append(limit + 1)
            rows = db.execute(
                "SELECT review_id,language,status,error_code,created_at,updated_at "
                "FROM reviews WHERE user_id=%s"
                + cursor_filter
                + " ORDER BY created_at DESC,review_id DESC LIMIT %s",
                params,
            ).fetchall()
            items = rows[:limit]
            return {
                "items": items,
                "next_cursor": items[-1]["review_id"] if len(rows) > limit else None,
            }

    @staticmethod
    def _lease(db, review_id: str, fence: int) -> dict | None:
        return db.execute(
            "SELECT * FROM reviews WHERE review_id=%s AND fence=%s AND status='running' "
            "AND lease_owner IS NOT NULL AND lease_until>extract(epoch FROM "
            "clock_timestamp()) FOR UPDATE",
            (review_id, fence),
        ).fetchone()

    def claim_review(
        self,
        review_id: str,
        owner: str,
        lease_seconds: float,
        expected_attempt: int | None = None,
        *,
        execution_timeout: float = 300,
    ) -> dict | None:
        with self.connection(write=True) as db:
            slot = db.execute(
                "SELECT * FROM inference_slots WHERE slot_id='default' FOR UPDATE"
            ).fetchone()
            row = db.execute(
                "SELECT * FROM reviews WHERE review_id=%s FOR UPDATE", (review_id,)
            ).fetchone()
            now = self._now(db)
            if (
                not row
                or row["status"] in ("completed", "failed")
                or row["execution_state"] == "recovery_blocked"
            ):
                return None
            recovery = (
                row["execution_state"] == "recovery_required"
                or row["rpc_request_id"] is not None
                or row["status"] == "running"
            )
            attempt = row["attempt"] if recovery else row["attempt"] + 1
            if expected_attempt is not None and expected_attempt != attempt:
                return None
            if slot["review_id"] not in (None, review_id):
                # Do not hold a Kafka partition behind another review's uncertain slot.
                # Persist a delayed redispatch without spending an execution attempt.
                if row["status"] == "queued" and (
                    row["execution_state"] == "queued" or row["available_at"] <= now
                ):
                    available_at = max(now + 1.0, row["available_at"])
                    db.execute(
                        "UPDATE reviews SET execution_state='retry_wait',available_at=%s,"
                        "updated_at=%s WHERE review_id=%s",
                        (available_at, now, review_id),
                    )
                    self._enqueue(db, row, now, row["attempt"] + 1, "slot_busy", available_at)
                return None
            if row["lease_until"] is not None and row["lease_until"] > now:
                return None
            if row["available_at"] > now:
                return None
            if not recovery and row["deadline_at"] <= now:
                db.execute(
                    "UPDATE reviews SET "
                    "status='failed',execution_state='failed',error_code='task_expired',"
                    "error_message='The task deadline expired before "
                    "execution.',updated_at=%s WHERE review_id=%s",
                    (now, review_id),
                )
                return None
            db.execute(
                "UPDATE inference_slots SET review_id=%s WHERE slot_id='default'", (review_id,)
            )
            return db.execute(
                "UPDATE reviews SET status='running',execution_state=%s,attempt=%s,retry_count=%s,"
                "fence=fence+1,lease_owner=%s,lease_until=%s,updated_at=%s,"
                "started_at=COALESCE(started_at,%s),"
                "execution_deadline_at=COALESCE(execution_deadline_at,LEAST(deadline_at,%s)) "
                "WHERE review_id=%s RETURNING *",
                (
                    "recovery_required" if recovery else "leased",
                    attempt,
                    max(0, attempt - 1),
                    owner,
                    now + lease_seconds,
                    now,
                    now,
                    now + execution_timeout,
                    review_id,
                ),
            ).fetchone()

    def delivery_disposition(self, review_id: str, expected_attempt: int) -> str:
        with self.connection() as db:
            row = db.execute("SELECT * FROM reviews WHERE review_id=%s", (review_id,)).fetchone()
            if not row:
                return "invalid"
            next_attempt = row["attempt"] + int(row["execution_state"] in ("queued", "retry_wait"))
            if row["status"] in ("completed", "failed"):
                # A queued task can expire before its first attempt starts.
                next_attempt = max(1, row["attempt"])
            if expected_attempt > next_attempt:
                return "invalid"
            if (
                row["status"] in ("completed", "failed")
                or row["execution_state"] == "recovery_blocked"
            ):
                return "done"
            if expected_attempt < next_attempt:
                return "done"
            if row["execution_state"] in ("retry_wait", "recovery_required"):
                # State and handoff are atomic; the scheduler republishes overdue work.
                saved = db.execute(
                    "SELECT 1 FROM outbox WHERE review_id=%s LIMIT 1", (review_id,)
                ).fetchone()
                if saved:
                    return "done"
            return "pending"

    def heartbeat(self, review_id: str, fence: int, lease_seconds: float) -> bool:
        with self.connection(write=True) as db:
            row = self._lease(db, review_id, fence)
            if not row:
                return False
            db.execute(
                "UPDATE reviews SET lease_until=%s,updated_at=%s WHERE review_id=%s",
                (self._now(db) + lease_seconds, self._now(db), review_id),
            )
            return True

    def record_rpc(
        self,
        review_id: str,
        fence: int,
        instance_id: str,
        request_id: str,
        section: str,
        *,
        model_id: str | None = None,
        revision: str | None = None,
    ) -> bool:
        with self.connection(write=True) as db:
            row = self._lease(db, review_id, fence)
            if not row or row["rpc_request_id"] is not None:
                return False
            if (row["model_id"] and model_id and row["model_id"] != model_id) or (
                row["model_revision"] and revision and row["model_revision"] != revision
            ):
                return False
            db.execute(
                "UPDATE reviews SET rpc_instance_id=%s,rpc_request_id=%s,rpc_section=%s,"
                "rpc_state='intent',execution_state='running',model_id=COALESCE(model_id,%s),"
                "model_revision=COALESCE(model_revision,%s),updated_at=%s WHERE review_id=%s",
                (instance_id, request_id, section, model_id, revision, self._now(db), review_id),
            )
            return True

    def save_section(
        self, review_id: str, fence: int, section: str, body: str, model_id: str, revision: str
    ) -> bool:
        with self.connection(write=True) as db:
            row = self._lease(db, review_id, fence)
            if not row or row["rpc_section"] != section or not row["rpc_request_id"]:
                return False
            sections = dict(row["sections"])
            sections[section] = body
            db.execute(
                "UPDATE reviews SET sections=%s,model_id=%s,model_revision=%s,rpc_instance_id=NULL,"
                "rpc_request_id=NULL,rpc_section=NULL,rpc_state=NULL,last_rpc_state='su"
                "cceeded',updated_at=%s WHERE review_id=%s",
                (Jsonb(sections), model_id, revision, self._now(db), review_id),
            )
            return True

    def mark_rpc_terminal(self, review_id: str, fence: int, state: str) -> bool:
        if state not in ("succeeded", "failed", "cancelled"):
            raise ValueError("A confirmed terminal remote state is required.")
        with self.connection(write=True) as db:
            if not self._lease(db, review_id, fence):
                return False
            db.execute(
                "UPDATE reviews SET "
                "rpc_instance_id=NULL,rpc_request_id=NULL,rpc_section=NULL,rpc_state=NU"
                "LL,"
                "last_rpc_state=%s,updated_at=%s WHERE review_id=%s",
                (state, self._now(db), review_id),
            )
            return True

    def finish_review(
        self,
        review_id: str,
        fence: int,
        *,
        result: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        model_id: str | None = None,
        revision: str | None = None,
    ) -> bool:
        with self.connection(write=True) as db:
            db.execute("SELECT * FROM inference_slots WHERE slot_id='default' FOR UPDATE")
            row = self._lease(db, review_id, fence)
            if not row or row["rpc_request_id"]:
                return False
            status = "failed" if error_code else "completed"
            db.execute(
                "UPDATE reviews SET "
                "status=%s,execution_state=%s,review_result=%s,error_code=%s,error_mess"
                "age=%s,"
                "model_id=COALESCE(%s,model_id),model_revision=COALESCE(%s,model_revisi"
                "on),lease_owner=NULL,"
                "lease_until=NULL,updated_at=%s WHERE review_id=%s",
                (
                    status,
                    status,
                    result,
                    error_code,
                    error_message,
                    model_id,
                    revision,
                    self._now(db),
                    review_id,
                ),
            )
            db.execute(
                "UPDATE inference_slots SET review_id=NULL WHERE slot_id='default' AND "
                "review_id=%s",
                (review_id,),
            )
            return True

    def defer_review(
        self,
        review_id: str,
        fence: int,
        error_code: str,
        delay: float,
        max_attempts: int,
        uncertain: bool = False,
        *,
        max_recovery_attempts: int = 8,
    ) -> bool:
        with self.connection(write=True) as db:
            db.execute("SELECT * FROM inference_slots WHERE slot_id='default' FOR UPDATE")
            row = self._lease(db, review_id, fence)
            if not row:
                return False
            now = self._now(db)
            # Any unresolved intent forces recovery, irrespective of caller classification.
            uncertain = uncertain or bool(row["rpc_request_id"])
            if uncertain:
                blocked = row["recovery_count"] + 1 >= max_recovery_attempts
                state = "recovery_blocked" if blocked else "recovery_required"
                db.execute(
                    "UPDATE reviews SET execution_state=%s,error_code='inference_stuck',"
                    "error_message='Remote execution requires reconciliation before "
                    "another generation.',"
                    "lease_owner=NULL,lease_until=NULL,available_at=%s,"
                    "recovery_count=recovery_count+1,updated_at=%s WHERE review_id=%s",
                    (state, now + delay, now, review_id),
                )
                if blocked:
                    self._emit_execution_dlq(db, row, now, "recovery_exhausted")
                else:
                    self._enqueue(db, row, now, row["attempt"], "recovery", now + delay)
                return True
            if row["attempt"] >= max_attempts or row["deadline_at"] <= now:
                db.execute(
                    "UPDATE reviews SET status='failed',execution_state='failed',error_code=%s,"
                    "error_message='Review could not complete within its retry or deadline "
                    "budget.',"
                    "lease_owner=NULL,lease_until=NULL,updated_at=%s WHERE review_id=%s",
                    (error_code, now, review_id),
                )
                self._emit_execution_dlq(db, row, now, error_code)
            else:
                db.execute(
                    "UPDATE reviews SET status='queued',execution_state='retry_wait',error_code=%s,"
                    "error_message='A retry is durably "
                    "scheduled.',lease_owner=NULL,lease_until=NULL,"
                    "available_at=%s,updated_at=%s WHERE review_id=%s",
                    (error_code, now + delay, now, review_id),
                )
                self._enqueue(db, row, now, row["attempt"] + 1, "retry", now + delay)
            db.execute(
                "UPDATE inference_slots SET review_id=NULL WHERE slot_id='default' AND "
                "review_id=%s",
                (review_id,),
            )
            return True

    def _emit_execution_dlq(self, db, row: dict, now: float, reason: str) -> None:
        event_id = str(uuid4())
        payload = {
            "event_id": event_id,
            "schema_version": 1,
            "review_id": row["review_id"],
            "attempt": row["attempt"],
            "execution_version": row["execution_version"],
            "trace_id": row["trace_id"],
            "reason": reason[:80],
            "created_at": now,
        }
        db.execute(
            "INSERT INTO "
            "outbox(event_id,review_id,topic,payload,available_at,created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, row["review_id"], self.dlq_topic, Jsonb(payload), now, now),
        )

    def recover_due(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        max_start_seconds: float = 60.0,
        *,
        max_recovery_attempts: int = 8,
    ) -> int:
        count = 0
        with self.connection(write=True) as db:
            db.execute("SELECT * FROM inference_slots WHERE slot_id='default' FOR UPDATE")
            now = self._now(db)
            rows = db.execute(
                "SELECT * FROM reviews WHERE status IN ('queued','running') "
                "AND execution_state <> 'recovery_blocked' AND "
                "((lease_until IS NOT NULL AND lease_until<=%s) OR "
                "(lease_until IS NULL AND available_at<=%s AND last_enqueued_at<=%s) OR "
                "(deadline_at<=%s AND rpc_request_id IS NULL AND lease_until IS NULL)) "
                "FOR UPDATE SKIP LOCKED",
                (now, now, now - max_start_seconds, now),
            ).fetchall()
            for row in rows:
                review_id = row["review_id"]
                if row["deadline_at"] <= now and not row["rpc_request_id"]:
                    db.execute(
                        "UPDATE reviews SET "
                        "status='failed',execution_state='failed',error_code='task_expired',"
                        "error_message='The task deadline "
                        "expired.',lease_owner=NULL,lease_until=NULL,fence=fence+1,updated_at=%"
                        "s WHERE review_id=%s",
                        (now, review_id),
                    )
                    db.execute(
                        "UPDATE inference_slots SET review_id=NULL WHERE review_id=%s", (review_id,)
                    )
                    self._emit_execution_dlq(db, row, now, "task_expired")
                elif row["status"] == "running":
                    # A crash with an expired active lease spends a recovery attempt;
                    # watchdog republication while no worker is available does not.
                    recovery_count = row["recovery_count"] + int(row["lease_until"] is not None)
                    blocked = recovery_count >= max_recovery_attempts
                    state = "recovery_blocked" if blocked else "recovery_required"
                    db.execute(
                        "UPDATE reviews SET execution_state=%s,lease_owner=NULL,lease_until=NULL,"
                        "fence=fence+1,available_at=%s,updated_at=%s,recovery_count=%s "
                        "WHERE review_id=%s",
                        (state, now + delay, now, recovery_count, review_id),
                    )
                    if blocked:
                        self._emit_execution_dlq(db, row, now, "recovery_exhausted")
                    else:
                        self._enqueue(db, row, now, row["attempt"], "lease_expired", now + delay)
                else:
                    self._enqueue(
                        db, row, now, row["attempt"] + 1, "watchdog", max(now, row["available_at"])
                    )
                count += 1
        return count

    def retry_recovery(self, review_id: str) -> bool:
        """Operator resumes reconciliation of the same remote identity, never a new start."""
        with self.connection(write=True) as db:
            db.execute("SELECT * FROM inference_slots WHERE slot_id='default' FOR UPDATE")
            row = db.execute(
                "SELECT * FROM reviews WHERE review_id=%s "
                "AND execution_state='recovery_blocked' FOR UPDATE",
                (review_id,),
            ).fetchone()
            if not row:
                return False
            now = self._now(db)
            db.execute(
                "UPDATE reviews SET execution_state='recovery_required',recovery_count=0,"
                "available_at=%s,updated_at=%s,fence=fence+1 WHERE review_id=%s",
                (now, now, review_id),
            )
            self._enqueue(db, row, now, row["attempt"], "operator_reconcile")
            return True

    def claim_outbox(self, owner: str, lease_seconds: float, limit: int = 10) -> list[dict]:
        with self.connection(write=True) as db:
            now = self._now(db)
            rows = db.execute(
                "SELECT * FROM outbox WHERE sent_at IS NULL AND available_at<=%s "
                "AND (claim_until IS NULL OR claim_until<=%s) ORDER BY "
                "created_at,event_id LIMIT %s FOR UPDATE SKIP LOCKED",
                (now, now, limit),
            ).fetchall()
            for row in rows:
                db.execute(
                    "UPDATE outbox SET "
                    "claim_owner=%s,claim_until=%s,publish_attempts=publish_attempts+1 "
                    "WHERE event_id=%s",
                    (owner, now + lease_seconds, row["event_id"]),
                )
                row["claim_owner"] = owner
                row["claim_until"] = now + lease_seconds
            return rows

    def mark_outbox_sent(self, event_id: str, owner: str) -> bool:
        with self.connection(write=True) as db:
            return (
                db.execute(
                    "UPDATE outbox SET "
                    "sent_at=%s,claim_owner=NULL,claim_until=NULL,last_error=NULL "
                    "WHERE event_id=%s AND claim_owner=%s AND claim_until>%s AND sent_at "
                    "IS NULL RETURNING event_id",
                    (self._now(db), event_id, owner, self._now(db)),
                ).fetchone()
                is not None
            )

    def release_outbox(self, event_id: str, owner: str, delay: float) -> bool:
        with self.connection(write=True) as db:
            return (
                db.execute(
                    "UPDATE outbox SET "
                    "claim_owner=NULL,claim_until=NULL,available_at=%s,last_error='publish_"
                    "unavailable' "
                    "WHERE event_id=%s AND claim_owner=%s AND sent_at IS NULL RETURNING event_id",
                    (self._now(db) + delay, event_id, owner),
                ).fetchone()
                is not None
            )

    def quarantine_message(
        self, topic: str, partition: int, offset: int, reason: str, payload: Any
    ) -> None:
        # Poison records retain references and a digest, never untrusted source/model payloads.
        raw = (
            payload
            if isinstance(payload, bytes)
            else json.dumps(payload, sort_keys=True, default=str).encode()
        )
        with self.connection(write=True) as db:
            now = self._now(db)
            inserted = db.execute(
                "INSERT INTO kafka_dlq VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO "
                "NOTHING RETURNING topic",
                (topic, partition, offset, reason[:80], hashlib.sha256(raw).hexdigest(), now),
            ).fetchone()
            if inserted:
                event_id = str(uuid4())
                metadata = {
                    "event_id": event_id,
                    "schema_version": 1,
                    "source_topic": topic,
                    "source_partition": partition,
                    "source_offset": offset,
                    "reason": reason[:80],
                    "created_at": now,
                }
                db.execute(
                    "INSERT INTO outbox(event_id,topic,payload,available_at,created_at) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (event_id, self.dlq_topic, Jsonb(metadata), now, now),
                )


class DatabaseRateLimiter:
    def __init__(self, store: Store) -> None:
        self.store = store

    def check(self, key: str, limit: int, window: float = 60) -> None:
        self.store.check_rate_limit(key, limit, window)
