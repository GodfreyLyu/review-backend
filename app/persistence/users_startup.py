"""Serial, cancellable startup retries for explicitly transient account transport errors."""

import asyncio
import logging
from collections.abc import Callable
from time import monotonic

from botocore.exceptions import (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from app.startup import failure_fields

logger = logging.getLogger("review")
USERS_STARTUP_BUDGET_SECONDS = 120.0
MAX_RETRY_WAIT_SECONDS = 10.0
TRANSIENT_CONNECTION_ERRORS = (
    ConnectTimeoutError,
    ReadTimeoutError,
    EndpointConnectionError,
    ConnectionClosedError,
)


class UsersStorageStartupTimeout(TimeoutError):
    """A successful response arrived after the startup budget had expired."""


async def wait_for_stop(shutdown: asyncio.Event, delay: float) -> None:
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=delay)
    except TimeoutError:
        pass


async def check_users_storage(ping: Callable[[], None], shutdown: asyncio.Event) -> bool:
    """Return false on shutdown; only repeat ping, never any other startup stage.

    Budget includes calls and waits. No call starts at/after the deadline. Await each
    SDK call fully (single attempt, 3s connect / 5s read) rather than abandoning a thread
    on an asyncio timeout. A call crossing the deadline is drained, then fails startup.
    Socket timeouts do not bound OS DNS resolution or scheduling; this is not a hard
    process wall-clock deadline. Shutdown follows the coordinator's existing grace.
    """
    started = monotonic()
    deadline = started + USERS_STARTUP_BUDGET_SECONDS
    attempt = 0
    delay = 1.0
    last_error: Exception | None = None

    def fields() -> dict:
        return {
            "stage": "users_storage",
            "startup_attempt": attempt,
            "startup_elapsed_ms": max(0, round((monotonic() - started) * 1000)),
        }

    def exhausted() -> None:
        error = last_error or UsersStorageStartupTimeout()
        logger.error("users_storage_retry_exhausted", extra={**fields(), **failure_fields(error)})
        raise error from None

    while not shutdown.is_set():
        if monotonic() >= deadline:
            exhausted()
        attempt += 1
        logger.info("users_storage_check_started", extra=fields())
        try:
            await asyncio.to_thread(ping)
        except TRANSIENT_CONNECTION_ERRORS as exc:
            last_error = exc
        else:
            if shutdown.is_set():
                break
            if monotonic() >= deadline:
                last_error = UsersStorageStartupTimeout()
                exhausted()
            logger.info("users_storage_ready", extra=fields())
            return True
        if shutdown.is_set():
            break
        remaining = deadline - monotonic()
        if remaining <= 0:
            exhausted()
        wait = min(delay, remaining)
        logger.warning(
            "users_storage_retry_scheduled",
            extra={
                **fields(),
                **failure_fields(last_error),
                "startup_wait_ms": round(wait * 1000),
            },
        )
        await wait_for_stop(shutdown, wait)
        delay = min(delay * 2, MAX_RETRY_WAIT_SECONDS)
    logger.info("users_storage_check_stopped", extra=fields())
    return False
