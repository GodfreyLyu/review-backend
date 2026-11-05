"""Durable review section orchestration, independent of HTTP and Kafka APIs."""

import logging
import time
from uuid import uuid4

from llm_contract.generated.ttypes import (
    GenerationOptions,
    GenerationRequest,
    LlmError,
    Message,
)
from llm_contract.generated.ttypes import GenerationState as State
from thrift.Thrift import TException

from app.errors import AppError, InferenceUnavailable, InferenceUncertain
from app.fault_injection import pause
from app.inference.generation import allocate_section_token_limits, derive_generation_seed
from app.inference.prompts import SECTION_SPECS, build_prompt_messages
from app.inference.review_output import (
    normalize_section_body,
    trim_capped_section_tail,
    validate_review_output,
)
from app.inference.rpc.client import ThriftReviewClient, app_error

logger = logging.getLogger("review.execution")
TERMINAL = {State.SUCCEEDED, State.FAILED, State.CANCELLED}


class LeaseLost(Exception):
    pass


class InvalidDelivery(ValueError):
    """A syntactically valid envelope cannot identify a valid execution delivery."""


class ReviewExecutor:
    def __init__(self, store, settings, rpc=None, owner=None):
        self.store, self.settings = store, settings
        self.rpc = rpc if rpc is not None else ThriftReviewClient(settings)
        self.owner = owner or str(uuid4())

    def handle(self, event, stop):
        job = self.store.claim_review(
            event.review_id,
            self.owner,
            self.settings.worker_lease_seconds,
            expected_attempt=event.attempt,
            execution_timeout=self.settings.inference_timeout_seconds,
        )
        if job is None:
            disposition = self.store.delivery_disposition(event.review_id, event.attempt)
            if disposition == "invalid":
                raise InvalidDelivery("invalid_review_or_attempt")
            return disposition == "done"
        job["deadline_at"] = min(
            job["deadline_at"], job.get("execution_deadline_at") or job["deadline_at"]
        )
        try:
            self._execute(job, stop)
            pause(self.settings, "REVIEW_TEST_PAUSE_AFTER_RESULT_FILE", stop)
            return True
        except LeaseLost:
            return False
        except InferenceUncertain:
            return self._defer(job, "inference_stuck", uncertain=True)
        except AppError as error:
            if job.get("rpc_request_id"):
                # Only a terminal snapshot or an explicit pre-admission rejection clears intent.
                return self._defer(job, "inference_stuck", uncertain=True)
            if error.code in {"inference_unavailable", "inference_draining", "interrupted"}:
                return self._defer(job, error.code)
            return self.store.finish_review(
                job["review_id"],
                job["fence"],
                error_code=error.code,
                error_message=error.message,
            )
        except Exception as exc:
            # DB failure: do not acknowledge. Expiry recovery owns persisted intent/slot.
            logger.warning(
                "review_dependency_failure",
                extra={
                    "review_id": job["review_id"],
                    "exception_type": type(exc).__name__,
                },
            )
            return False

    def _defer(self, job, code, uncertain=False):
        delay = min(
            self.settings.retry_max_seconds,
            self.settings.retry_base_seconds
            * 2 ** min(job.get("recovery_count", 0) if uncertain else job["attempt"] - 1, 10),
        )
        return self.store.defer_review(
            job["review_id"],
            job["fence"],
            code,
            delay,
            self.settings.max_retries + 1,
            uncertain=uncertain,
            max_recovery_attempts=self.settings.max_recovery_attempts,
        )

    def _heartbeat(self, job):
        if not self.store.heartbeat(
            job["review_id"], job["fence"], self.settings.worker_lease_seconds
        ):
            raise LeaseLost

    def _execute(self, job, stop):
        self._heartbeat(job)
        # Reconcile the exact persisted instance/request before handshaking a new instance.
        if job.get("rpc_request_id"):
            snapshot = self._drive(job, stop, request=None)
            self._accept_snapshot(job, snapshot)
        if stop.is_set():
            raise AppError("interrupted", "Review execution was interrupted.", 503)
        if time.time() >= job["deadline_at"]:
            raise AppError("inference_timeout", "Review deadline expired.", 504)
        info = self.rpc.connect()
        if job.get("model_id") and (
            job.get("model_id") != info.model_id or job.get("model_revision") != info.model_revision
        ):
            raise AppError("ollama_model_mismatch", "Review model identity changed.", 503)
        limits = allocate_section_token_limits(self.settings.model_max_output_tokens)
        for section, _title, _instruction in SECTION_SPECS:
            if section in job["sections"]:
                continue
            self._heartbeat(job)
            if stop.is_set():
                raise AppError("interrupted", "Review execution was interrupted.", 503)
            remaining = min(
                job["deadline_at"] - time.time(),
                self.settings.inference_timeout_seconds,
                info.limits.max_deadline_ms / 1000,
            )
            if remaining <= 0:
                raise AppError("inference_timeout", "Review deadline expired.", 504)
            request_id = str(uuid4())
            if not self.store.record_rpc(
                job["review_id"],
                job["fence"],
                info.instance_id,
                request_id,
                section,
                model_id=info.model_id,
                revision=info.model_revision,
            ):
                raise LeaseLost
            job.update(
                rpc_instance_id=info.instance_id,
                rpc_request_id=request_id,
                rpc_section=section,
                model_id=info.model_id,
                model_revision=info.model_revision,
            )
            request = GenerationRequest(
                request_id=request_id,
                instance_id=info.instance_id,
                messages=[
                    Message(**value)
                    for value in build_prompt_messages(job["source_code"], job["language"], section)
                ],
                options=GenerationOptions(
                    max_output_tokens=limits[section],
                    temperature=self.settings.model_temperature,
                    top_p=self.settings.model_top_p,
                    seed=derive_generation_seed(
                        info.model_revision, job["language"], section, job["source_code"]
                    ),
                ),
                deadline_ms=max(1, int(remaining * 1000)),
            )
            snapshot = self._drive(job, stop, request)
            self._accept_snapshot(job, snapshot, limits[section])
        text = "\n\n".join(
            f"## {title}\n{job['sections'][section]}" for section, title, _ in SECTION_SPECS
        )
        result = validate_review_output(text, job["source_code"])
        if stop.is_set():
            raise AppError("interrupted", "Review execution was interrupted.", 503)
        if time.time() >= job["deadline_at"]:
            raise AppError("inference_timeout", "Review deadline expired.", 504)
        if not self.store.finish_review(
            job["review_id"],
            job["fence"],
            result=result,
            model_id=job["model_id"],
            revision=job["model_revision"],
        ):
            raise LeaseLost

    def _clear_rpc(self, job, state):
        if not self.store.mark_rpc_terminal(job["review_id"], job["fence"], state):
            raise LeaseLost
        job.update(rpc_instance_id=None, rpc_request_id=None, rpc_section=None)

    def _accept_snapshot(self, job, snapshot, token_limit=None):
        if snapshot.state != State.SUCCEEDED or snapshot.result is None or snapshot.error:
            self._clear_rpc(job, "cancelled" if snapshot.state == State.CANCELLED else "failed")
            if snapshot.error:
                raise app_error(snapshot.error)
            raise AppError("inference_failed", "The model could not complete this review.", 502)
        result, section = snapshot.result, job["rpc_section"]
        if (
            not result.model_id
            or not result.model_revision
            or result.usage is None
            or not isinstance(result.content, str)
            or type(result.usage.input_tokens) is not int
            or type(result.usage.output_tokens) is not int
            or result.usage.input_tokens < 0
            or result.usage.output_tokens < 0
        ):
            self._clear_rpc(job, "succeeded")
            raise AppError("invalid_model_response", "Invalid inference result metadata.", 502)
        if job.get("model_id") and (
            job.get("model_id") != result.model_id
            or job.get("model_revision") != result.model_revision
        ):
            self._clear_rpc(job, "succeeded")
            raise AppError("ollama_model_mismatch", "Review model identity changed.", 503)
        try:
            body = normalize_section_body(section, result.content)
            body, _ = trim_capped_section_tail(
                body,
                result.finish_reason == "length"
                or (token_limit is not None and result.usage.output_tokens >= token_limit),
            )
        except AppError:
            self._clear_rpc(job, "succeeded")
            raise
        if not self.store.save_section(
            job["review_id"],
            job["fence"],
            section,
            body,
            result.model_id,
            result.model_revision,
        ):
            raise LeaseLost
        job["sections"][section] = body
        job.update(
            model_id=result.model_id,
            model_revision=result.model_revision,
            rpc_instance_id=None,
            rpc_request_id=None,
            rpc_section=None,
        )

    def _drive(self, job, stop, request):
        method = "start" if request is not None else "get"
        if request is not None and stop.is_set():
            # This process knows it has not sent the persisted intent yet.
            self._clear_rpc(job, "failed")
            raise AppError("interrupted", "Review execution was interrupted.", 503)
        uncertain_since = None
        cancel_since = None
        # Recovery reads/cancels only: it never repeats start, even for not_found.
        while True:
            self._heartbeat(job)
            if (stop.is_set() or time.time() >= job["deadline_at"]) and method != "start":
                method = "cancel"
                cancel_since = cancel_since or time.monotonic()
            try:
                if method == "start":
                    snapshot = self.rpc.start(request)
                else:
                    snapshot = getattr(self.rpc, method)(
                        job["rpc_instance_id"], job["rpc_request_id"]
                    )
                uncertain_since = None
            except LlmError as error:
                if method != "start":
                    raise InferenceUncertain() from None
                # An explicit rejected start confirms no admission; a lost reply does not.
                self._clear_rpc(job, "failed")
                if error.code == "instance_changed":
                    raise InferenceUnavailable() from None
                raise app_error(error) from None
            except InferenceUnavailable:
                if method == "start":
                    # Connection.open failed before any generated call could send bytes.
                    self._clear_rpc(job, "failed")
                    raise
                uncertain_since = uncertain_since or time.monotonic()
            except (TException, OSError):
                uncertain_since = uncertain_since or time.monotonic()
            else:
                if (
                    snapshot.instance_id != job["rpc_instance_id"]
                    or snapshot.request_id != job["rpc_request_id"]
                ):
                    raise InferenceUncertain()
                if snapshot.state in TERMINAL:
                    if snapshot.state == State.CANCELLED and time.time() < job["deadline_at"]:
                        self._clear_rpc(job, "cancelled")
                        raise AppError("interrupted", "Review execution was interrupted.", 503)
                    return snapshot
                if snapshot.state not in {State.QUEUED, State.RUNNING, State.CANCELLING}:
                    raise InferenceUncertain()
            if uncertain_since and (
                time.monotonic() - uncertain_since >= self.settings.thrift_recovery_seconds
            ):
                raise InferenceUncertain()
            if cancel_since and (
                time.monotonic() - cancel_since >= self.settings.thrift_recovery_seconds
            ):
                raise InferenceUncertain()
            method = "cancel" if stop.is_set() or time.time() >= job["deadline_at"] else "get"
            # Event.wait would spin on an already-set stop while cancellation reconciles.
            time.sleep(min(0.1, self.settings.worker_poll_seconds))
