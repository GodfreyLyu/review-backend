"""User persistence in PostgreSQL; credentials are never included in review messages."""

import time
from typing import Protocol
from uuid import uuid4

from psycopg.errors import UniqueViolation

from app.domain import UserRecord
from app.errors import AppError
from app.persistence.storage import Store


class UserStore(Protocol):
    def ping(self) -> None: ...
    def get(self, login_id: str) -> UserRecord | None: ...
    def create(self, login_id: str, password_hash: str) -> UserRecord: ...


class PostgresUsers:
    def __init__(self, store: Store) -> None:
        self.store = store

    def ping(self) -> None:
        self.store.ping()

    def get(self, login_id: str) -> UserRecord | None:
        with self.store.connection() as db:
            return db.execute("SELECT * FROM users WHERE login_id=%s", (login_id,)).fetchone()

    def create(self, login_id: str, password_hash: str) -> UserRecord:
        try:
            with self.store.connection(write=True) as db:
                return db.execute(
                    "INSERT INTO users(login_id,user_id,password_hash,created_at) VALUES "
                    "(%s,%s,%s,%s) RETURNING *",
                    (login_id, str(uuid4()), password_hash, str(time.time())),
                ).fetchone()
        except UniqueViolation:
            raise AppError(
                "login_unavailable", "This login identifier is unavailable.", 409
            ) from None
