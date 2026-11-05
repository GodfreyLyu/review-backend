"""Versioned SQLite storage. Each operation owns a short-lived connection and transaction."""

import hashlib
import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from typing import cast
from uuid import uuid4

from app.domain import (
    CreateReviewResult,
    HistoryRecord,
    ReviewRecord,
    ReviewSummaryRecord,
    SessionRecord,
    UserRecord,
)
from app.errors import AppError

SCHEMA = """
CREATE TABLE reviews (
  review_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, client_request_id TEXT NOT NULL,
  language TEXT NOT NULL, source_code TEXT NOT NULL, review_result TEXT,
  status TEXT NOT NULL CHECK(status IN ('queued','running','completed','failed')),
  error_code TEXT, error_message TEXT, model_id TEXT, model_revision TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, updated_at REAL NOT NULL,
  UNIQUE(user_id, client_request_id)
);
CREATE INDEX reviews_user_created ON reviews(user_id, created_at DESC, review_id DESC);
CREATE INDEX reviews_status_created ON reviews(status, created_at);
CREATE TABLE sessions (
  token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL, login_id TEXT NOT NULL,
  csrf_token TEXT NOT NULL, expires_at REAL NOT NULL
);
CREATE INDEX sessions_expiry ON sessions(expires_at);
PRAGMA user_version = 1;
"""


class Store:
    def __init__(self, directory: Path) -> None:
        self.path = directory / "reviews.sqlite3"

    @contextmanager
    def connection(self, write: bool = False) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("PRAGMA synchronous=FULL")
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if write:
                connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version == 0:
                db.executescript("BEGIN IMMEDIATE;\n" + SCHEMA + "\nCOMMIT;")
            elif version != 1:
                raise RuntimeError("Unsupported database schema version.")
        self.path.chmod(0o600)

    def ping(self) -> None:
        with self.connection(write=True) as db:
            db.execute("DELETE FROM sessions WHERE expires_at <= ?", (time.time(),))

    def session_create(
        self, token: str, user: UserRecord, csrf_token: str, expires_at: float
    ) -> None:
        with self.connection(write=True) as db:
            db.execute("DELETE FROM sessions WHERE expires_at <= ?", (time.time(),))
            db.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
                (self.digest(token), user["user_id"], user["login_id"], csrf_token, expires_at),
            )

    @staticmethod
    def digest(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    def session_get(self, token: str) -> SessionRecord | None:
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM sessions WHERE token_hash = ? AND expires_at > ?",
                (self.digest(token), time.time()),
            ).fetchone()
            return cast(SessionRecord, dict(row)) if row else None

    def session_delete(self, token: str) -> None:
        with self.connection(write=True) as db:
            db.execute("DELETE FROM sessions WHERE token_hash = ?", (self.digest(token),))

    def create_review(
        self, user_id: str, request_id: str, language: str, source: str, capacity: int
    ) -> CreateReviewResult:
        """Deduplicate, check capacity and enqueue under the same write lock."""
        with self.connection(write=True) as db:
            existing = db.execute(
                "SELECT * FROM reviews WHERE user_id = ? AND client_request_id = ?",
                (user_id, request_id),
            ).fetchone()
            # A retry must find its saved job even if the queue has since filled up.
            if existing:
                if existing["source_code"] != source or existing["language"] != language:
                    raise AppError(
                        "idempotency_conflict", "This request key has different input.", 409
                    )
                return CreateReviewResult(review=cast(ReviewRecord, dict(existing)), created=False)

            # Capacity counts waiting jobs; the single running job gets one extra slot.
            active_count = db.execute(
                "SELECT count(*) FROM reviews WHERE status IN ('queued','running')"
            ).fetchone()[0]
            if active_count >= capacity + 1:
                raise AppError("queue_full", "The review queue is full. Try again later.", 429)
            queued_count = db.execute(
                "SELECT count(*) FROM reviews WHERE status = 'queued'"
            ).fetchone()[0]
            if queued_count >= capacity:
                raise AppError("queue_full", "The review queue is full. Try again later.", 429)
            if db.execute(
                "SELECT 1 FROM reviews WHERE user_id = ? AND status IN ('queued','running')",
                (user_id,),
            ).fetchone():
                raise AppError("review_active", "A review is already active for your account.", 409)

            review_id = str(uuid4())
            now = time.time()
            db.execute(
                """
                INSERT INTO reviews (
                    review_id, user_id, client_request_id, language, source_code,
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)
                """,
                (review_id, user_id, request_id, language, source, now, now),
            )
            row = db.execute("SELECT * FROM reviews WHERE review_id=?", (review_id,)).fetchone()
            return CreateReviewResult(
                review=cast(ReviewRecord, dict(row)), created=True, queue_depth=queued_count + 1
            )

    def queue_depth(self) -> int:
        """Return only the queued count; never read or copy review content."""

        with self.connection() as db:
            return db.execute("SELECT count(*) FROM reviews WHERE status='queued'").fetchone()[0]

    def queue_snapshot(self) -> tuple[int, float | None]:
        """Observe queue size and age without creating a database or taking a write lock."""
        uri = self.path.resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=0.1)) as db:
            return db.execute(
                "SELECT count(*), min(created_at) FROM reviews WHERE status='queued'"
            ).fetchone()

    def get_review(self, user_id: str, review_id: str) -> ReviewRecord:
        with self.connection() as db:
            row = db.execute(
                "SELECT * FROM reviews WHERE user_id=? AND review_id=?", (user_id, review_id)
            ).fetchone()
            if not row:
                raise AppError("review_not_found", "Review not found.", 404)
            return cast(ReviewRecord, dict(row))

    def history(self, user_id: str, limit: int, before: str | None = None) -> HistoryRecord:
        with self.connection() as db:
            parameters: list[str | float | int] = [user_id]
            cursor_filter = ""
            if before:
                cursor = db.execute(
                    "SELECT created_at FROM reviews WHERE user_id=? AND review_id=?",
                    (user_id, before),
                ).fetchone()
                if not cursor:
                    raise AppError("invalid_cursor", "Invalid history cursor.", 400)
                # Review IDs break ties when multiple rows share a creation timestamp.
                cursor_filter = " AND (created_at,review_id) < (?,?)"
                parameters.extend([cursor[0], before])
            # Read one extra row to determine whether another page exists.
            parameters.append(limit + 1)
            rows = db.execute(
                "SELECT review_id,language,status,error_code,created_at,updated_at FROM reviews "
                "WHERE user_id=?"
                + cursor_filter
                + " ORDER BY created_at DESC,review_id DESC LIMIT ?",
                parameters,
            ).fetchall()
            items = [cast(ReviewSummaryRecord, dict(row)) for row in rows[:limit]]
            return {
                "items": items,
                "next_cursor": items[-1]["review_id"] if len(rows) > limit else None,
            }

    def recover(self, max_retries: int, capacity: int) -> None:
        """Fail exhausted attempts, requeue interrupted work and enforce current capacity."""
        with self.connection(write=True) as db:
            now = time.time()
            db.execute(
                """
                UPDATE reviews
                SET status = 'failed', error_code = 'interrupted',
                    error_message = 'Review interrupted; retry limit reached.', updated_at = ?
                WHERE status = 'running' AND retry_count >= ?
                """,
                (now, max_retries),
            )
            # Only running rows below the retry limit remain after the update above.
            db.execute(
                """
                UPDATE reviews
                SET status = 'queued', retry_count = retry_count + 1, updated_at = ?
                WHERE status = 'running'
                """,
                (now,),
            )
            # If capacity shrank, preserve the oldest queued submissions first.
            db.execute(
                """
                UPDATE reviews
                SET status = 'failed', error_code = 'queue_recovery_limit',
                    error_message = 'Queue capacity was reduced. Submit a new review.',
                    updated_at = ?
                WHERE review_id IN (
                    SELECT review_id FROM reviews WHERE status = 'queued'
                    ORDER BY created_at LIMIT -1 OFFSET ?
                )
                """,
                (now, capacity),
            )

    def claim(self) -> ReviewRecord | None:
        """Select the oldest queued job and mark it running in one transaction."""
        with self.connection(write=True) as db:
            row = db.execute(
                "SELECT * FROM reviews WHERE status='queued' ORDER BY created_at LIMIT 1"
            ).fetchone()
            if not row:
                return None
            db.execute(
                "UPDATE reviews SET status='running',updated_at=? WHERE review_id=?",
                (time.time(), row["review_id"]),
            )
            return cast(ReviewRecord, dict(row))

    def finish(
        self,
        review_id: str,
        *,
        result: str | None = None,
        error: AppError | None = None,
        model_id: str | None = None,
        revision: str | None = None,
    ) -> None:
        with self.connection(write=True) as db:
            db.execute(
                "UPDATE reviews SET status=?,review_result=?,error_code=?,error_message=?,"
                "model_id=?,model_revision=?,updated_at=? WHERE review_id=? AND status='running'",
                (
                    "failed" if error else "completed",
                    result,
                    error.code if error else None,
                    error.message if error else None,
                    model_id,
                    revision,
                    time.time(),
                    review_id,
                ),
            )
