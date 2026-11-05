import asyncio
import logging
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum

from app.config import Settings
from app.domain import ReviewRecord
from app.errors import AppError, InferenceUnavailable, InferenceUncertain
from app.inference.client import ReviewClient
from app.inference.identity import model_identity
from app.monitoring import Metrics
from app.persistence.storage import Store
from app.persistence.users import UserStore
from app.persistence.users_startup import check_users_storage
from app.startup import failure_fields

logger = logging.getLogger("review")
INFERENCE_HEALTH_INTERVAL_SECONDS = 5.0
INFERENCE_RETRY_INITIAL_SECONDS = 1.0
INFERENCE_RETRY_MAX_SECONDS = 10.0


def milliseconds_since(started_at: float, *, now: float | None = None) -> int:
    return max(0, round(((time.time() if now is None else now) - started_at) * 1000))


class CoordinatorState(StrEnum):
    INITIALIZING = "initializing"
    CONNECTING = "inference_connecting"
    READY = "ready"
    DRAINING = "inference_draining"
    STUCK = "inference_stuck"
    FAILED = "startup_or_storage_failure"


class Coordinator:
    def __init__(
        self,
        store: Store,
        users: UserStore,
        review_client: ReviewClient,
        settings: Settings,
        metrics: Metrics,
    ) -> None:
        self.store = store
        self.users = users
        self.review_client = review_client
        self.settings = settings
        self.metrics = metrics
        self._state = CoordinatorState.INITIALIZING
        self._closing = False
        self._shutdown = asyncio.Event()
        self.stop = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="inference")

    @property
    def state(self) -> CoordinatorState:
        return self._state

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def ready(self) -> bool:
        return self.state is CoordinatorState.READY and not self.closing

    @property
    def live(self) -> bool:
        return self.state not in {CoordinatorState.FAILED, CoordinatorState.STUCK}

    def _transition(self, state: CoordinatorState) -> None:
        self._state = state
        self.metrics.coordinator_state(state)

    def start(self) -> None:
        self.task = asyncio.create_task(self.run())

    async def run(self) -> None:
        stage = "storage_initialize"
        try:
            await asyncio.to_thread(self.store.initialize)
            stage = "users_storage"
            if not await check_users_storage(self.users.ping, self._shutdown):
                return
            stage = "queue_recovery"
            await asyncio.to_thread(
                self.store.recover, self.settings.max_retries, self.settings.queue_capacity
            )
            if self.closing:
                return
            self._transition(CoordinatorState.CONNECTING)
            next_health_check = 0.0
            while not self.closing and self.state is not CoordinatorState.STUCK:
                if (
                    self.state is CoordinatorState.CONNECTING
                    or time.monotonic() >= next_health_check
                ):
                    stage = "inference_handshake"
                    if not await self.connect_inference():
                        return
                    if self.state is not CoordinatorState.READY:
                        stage = "storage_recheck"
                        await asyncio.to_thread(self.store.ping)
                        if self.closing:
                            return
                        self._transition(CoordinatorState.READY)
                        logger.info("inference_ready")
                    next_health_check = time.monotonic() + INFERENCE_HEALTH_INTERVAL_SECONDS
                if self.closing:
                    return
                stage = "queue_processing"
                job = await asyncio.to_thread(self.store.claim)
                if job is None:
                    await self.wait_for_shutdown(0.1)
                    continue
                await self.process(job)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._transition(CoordinatorState.FAILED)
            logger.error("coordinator_failed", extra={"stage": stage, **failure_fields(exc)})

    async def connect_inference(self) -> bool:
        """Retry only dependency unavailability, without replaying any review or generation."""
        delay = INFERENCE_RETRY_INITIAL_SECONDS
        while not self.closing:
            try:
                # Share the review executor: a handshake must never replace identity mid-review.
                await asyncio.get_running_loop().run_in_executor(
                    self.executor, self.review_client.connect
                )
            except InferenceUnavailable as exc:
                if self.closing:
                    return False
                self._transition(CoordinatorState.CONNECTING)
                logger.warning(
                    "inference_reconnect_scheduled",
                    extra={"stage": "inference_handshake", **failure_fields(exc)},
                )
                await self.wait_for_shutdown(delay)
                delay = min(delay * 2, INFERENCE_RETRY_MAX_SECONDS)
            else:
                return not self.closing
        return False

    async def wait_for_shutdown(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._shutdown.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def process(self, job: ReviewRecord) -> None:
        self.stop.clear()
        await self.log_started(job)
        future = asyncio.get_running_loop().run_in_executor(
            self.executor,
            self.execute,
            job["source_code"],
            job["language"],
            self.stop,
        )
        result: str | None = None
        error: AppError | None = None
        timed_out = False
        try:
            # Keep the executor future alive after the request deadline so drain()
            # can wait for the actual thread exit before accepting more work.
            result = await asyncio.wait_for(
                asyncio.shield(future), self.settings.inference_timeout_seconds
            )
            if not result or not result.strip():
                raise AppError(
                    "empty_model_response", "The model returned no review. Please retry."
                )
        except TimeoutError:
            self.stop.set()
            self._transition(CoordinatorState.DRAINING)
            timed_out = True
            error = AppError("inference_timeout", "Review timed out. Try a shorter submission.")
        except InferenceUncertain as exc:
            self._transition(CoordinatorState.STUCK)
            error = exc
        except InferenceUnavailable as exc:
            self._transition(CoordinatorState.CONNECTING)
            error = exc
        except AppError as exc:
            error = exc
        except Exception:
            error = AppError("inference_failed", "The model could not complete this review.")
        # Persist the timeout for the client before waiting for the worker to stop.
        await self.finish(job, result, error)
        if timed_out:
            await self.drain(future, job["review_id"])

    def execute(self, source: str, language: str, stop: threading.Event) -> str:
        """Observe the actual executor lifetime, including a late exit after timeout."""
        started_at = time.monotonic()
        self.metrics.worker_started()
        try:
            return self.review_client.review(source, language, stop)
        finally:
            self.metrics.worker_finished(time.monotonic() - started_at)

    async def log_started(self, job: ReviewRecord) -> None:
        queue_wait_ms = milliseconds_since(job["created_at"])
        self.metrics.started(queue_wait_ms / 1000)
        start_fields: dict[str, str | int] = {
            "review_id": job["review_id"],
            "outcome": "accepted",
            "error_code": "none",
            "queue_wait_ms": queue_wait_ms,
        }
        try:
            start_fields["queue_depth"] = await asyncio.to_thread(self.store.queue_depth)
        except sqlite3.Error:
            # Observability must not interrupt a review after it has been claimed.
            pass
        logger.info(
            "review_started",
            extra=start_fields,
        )

    async def finish(self, job: ReviewRecord, result: str | None, error: AppError | None) -> None:
        identity = model_identity(self.review_client)
        await asyncio.to_thread(
            self.store.finish,
            job["review_id"],
            result=result,
            error=error,
            model_id=identity["model_id"] if not error else None,
            revision=identity["model_revision"] if not error else None,
        )
        duration_ms = milliseconds_since(job["created_at"])
        self.metrics.finished(error.code if error else "completed", duration_ms / 1000)
        log_fields: dict[str, str | int] = {
            "review_id": job["review_id"],
            "outcome": error.code if error else "completed",
            "error_code": error.code if error else "none",
            "duration_ms": duration_ms,
        }
        if error and error.validation_reason is not None:
            log_fields["validation_reason"] = error.validation_reason.value
        logger.info(
            "review_finished",
            extra=log_fields,
        )

    async def drain(self, future: asyncio.Future[str], review_id: str) -> None:
        # A timed-out Python thread cannot be killed. Never start another inference over it.
        try:
            await asyncio.wait_for(asyncio.shield(future), self.settings.inference_drain_seconds)
        except TimeoutError:
            self._transition(CoordinatorState.STUCK)
            logger.error(
                "inference_stuck",
                extra={
                    "review_id": review_id,
                    "outcome": "inference_stuck",
                    "error_code": "inference_stuck",
                },
            )
            future.add_done_callback(
                lambda done: done.exception() if not done.cancelled() else None
            )
            return
        except InferenceUncertain:
            self._transition(CoordinatorState.STUCK)
            return
        except InferenceUnavailable:
            self._transition(CoordinatorState.CONNECTING)
            return
        except Exception:
            pass
        if not self.closing:
            self._transition(CoordinatorState.READY)

    async def close(self) -> None:
        self._closing = True
        self._shutdown.set()
        self.stop.set()
        self.review_client.close()
        try:
            if not self.task.done():
                await asyncio.wait_for(
                    asyncio.shield(self.task), self.settings.shutdown_grace_seconds
                )
        except TimeoutError:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        finally:
            self.executor.shutdown(wait=False, cancel_futures=True)
