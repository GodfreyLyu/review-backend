"""Backend review orchestration over an application-neutral generated Thrift client."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import TYPE_CHECKING

from llm_contract.generated.ttypes import (
    GenerationOptions,
    GenerationRequest,
    LlmError,
    Message,
    ModelInfo,
)
from llm_contract.generated.ttypes import (
    GenerationState as State,
)
from thrift.Thrift import TException
from thrift.transport.TTransport import TTransportException

from app.errors import AppError, InferenceUnavailable, InferenceUncertain
from app.inference.generation import (
    GenerationMetrics,
    SectionMetrics,
    allocate_section_token_limits,
    check_deadline,
    derive_generation_seed,
)
from app.inference.prompts import SECTION_SPECS, build_prompt_messages
from app.inference.review_output import (
    normalize_section_body,
    trim_capped_section_tail,
    validate_review_output,
)
from app.inference.rpc.transport import Connection

if TYPE_CHECKING:
    from app.inference.settings import InferenceSettings
    from app.monitoring import Metrics

logger = logging.getLogger("review")
TERMINAL = {State.SUCCEEDED, State.FAILED, State.CANCELLED}


def app_error(error: LlmError) -> AppError:
    # Public HTTP errors remain application-owned. Never trust remote message text.
    if error.code == "input_limit":
        return AppError("token_limit", "Source exceeds the model input token limit.", 422)
    if error.code in {"cancelled", "deadline_exceeded", "shutdown"}:
        return AppError("inference_timeout", "Review timed out. Try a shorter submission.", 504)
    if error.code in {"overloaded", "retention_full"}:
        return AppError("inference_draining", "Inference service is temporarily unavailable.", 503)
    if error.code in {"unavailable", "ollama_unavailable"}:
        return InferenceUnavailable()
    if error.code == "invalid_response":
        return AppError("invalid_model_response", "The model returned an invalid response.", 502)
    if error.code == "model_missing":
        return AppError("ollama_model_missing", "The configured model is not installed.", 503)
    if error.code in {"model_mismatch", "gpu_required"}:
        return AppError(
            "ollama_model_mismatch", "Inference model does not match configuration.", 503
        )
    if error.code == "invalid_request":
        return AppError("request_rejected", "Generation options exceed service limits.", 422)
    return AppError("inference_failed", "The model could not complete this review.", 502)


class ThriftReviewClient:
    def __init__(self, settings: InferenceSettings, *, metrics: Metrics | None = None):
        self.settings = settings.model_copy(deep=True)
        self.metrics = metrics
        self.info: ModelInfo | None = None
        self.digest = self.device = self.quantization = None
        self.closing = threading.Event()

    def _call(self, method, *args):
        with Connection(self.settings) as client:
            return getattr(client, method)(*args)

    def connect(self) -> None:
        """One read-only handshake. The coordinator owns retries and shutdown waits."""
        try:
            if self.closing.is_set():
                raise InferenceUnavailable()
            info = self._call("getModelInfo")
            health = self._call("getHealth")
            if self.closing.is_set() or not health.ready or health.draining:
                raise InferenceUnavailable()
            if not info.instance_id or not info.model_id or not info.model_revision:
                raise AppError("inference_failed", "Invalid inference service metadata.", 503)
            if (
                self.settings.expected_model_id and info.model_id != self.settings.expected_model_id
            ) or (
                self.settings.expected_model_revision
                and info.model_revision != self.settings.expected_model_revision
            ):
                raise AppError("ollama_model_mismatch", "Unexpected inference model identity.", 503)
        except (InferenceUnavailable, TTransportException, OSError):
            self._forget_connection()
            raise InferenceUnavailable() from None
        except TException:
            self._forget_connection()
            raise AppError(
                "inference_failed", "Incompatible inference service protocol.", 503
            ) from None
        except AppError:
            self._forget_connection()
            raise
        self.info = info
        self.digest, self.device, self.quantization = (
            info.model_revision,
            info.device,
            info.quantization,
        )

    def _forget_connection(self) -> None:
        self.info = None
        self.digest = self.device = self.quantization = None

    def _generate(self, messages, options, deadline, stop):
        info = self.info
        request = GenerationRequest(
            request_id=str(uuid.uuid4()),
            instance_id=info.instance_id,
            messages=[Message(**m) for m in messages],
            options=options,
            deadline_ms=max(
                1, min(info.limits.max_deadline_ms, int((deadline - time.monotonic()) * 1000))
            ),
        )
        uncertain_since = None
        method, args = "startGeneration", (request,)
        cancel_requested = False
        cancelled_at = None
        while True:
            try:
                snapshot = self._call(method, *args)
                uncertain_since = None
            except LlmError as error:
                if method != "startGeneration":
                    # A lookup/cancel error cannot establish whether admitted work stopped.
                    raise InferenceUncertain() from None
                if error.code == "instance_changed":
                    # The service fenced this start before admission; reconnect for later jobs.
                    raise InferenceUnavailable() from None
                raise app_error(error) from None
            except (TException, OSError, InferenceUnavailable) as error:
                if isinstance(error, InferenceUnavailable) and method == "startGeneration":
                    # Connection.open failed before the request could be sent.
                    raise
                # A lost start response is recovered by ID; start is never resent.
                uncertain_since = uncertain_since or time.monotonic()
                if time.monotonic() - uncertain_since >= self.settings.thrift_recovery_seconds:
                    raise InferenceUncertain() from None
                method, args = "getGeneration", (request.instance_id, request.request_id)
                # Shutdown still needs reconciliation; an already-set close event must
                # not turn failed lookups into a busy retry loop.
                time.sleep(0.1)
                continue
            if (
                snapshot.instance_id != request.instance_id
                or snapshot.request_id != request.request_id
            ):
                raise InferenceUncertain()
            if snapshot.state in TERMINAL:
                if snapshot.error:
                    raise app_error(snapshot.error)
                result = snapshot.result
                if (
                    snapshot.state != State.SUCCEEDED
                    or result is None
                    or result.model_revision != self.digest
                    or result.model_id != info.model_id
                ):
                    raise InferenceUncertain()
                check_deadline(stop, deadline)
                if cancel_requested or self.closing.is_set():
                    raise AppError("inference_timeout", "Review was cancelled.", 504)
                return result
            if snapshot.state not in {State.QUEUED, State.RUNNING, State.CANCELLING}:
                raise InferenceUncertain()
            if (
                cancelled_at is not None
                and time.monotonic() - cancelled_at >= self.settings.thrift_recovery_seconds
            ):
                # Give up with an uncertain outcome, never a false cancellation success.
                # This also bounds client-thread cleanup during backend shutdown.
                raise InferenceUncertain()
            if stop.is_set() or self.closing.is_set() or time.monotonic() >= deadline:
                method, args = "cancelGeneration", (request.instance_id, request.request_id)
                cancel_requested = True
                cancelled_at = cancelled_at or time.monotonic()
            else:
                method, args = "getGeneration", (request.instance_id, request.request_id)
            # Continue until a terminal snapshot confirms native model exit.
            time.sleep(0.05)

    def review(self, source, language, stop):
        if self.info is None or self.closing.is_set():
            raise InferenceUnavailable()
        deadline = time.monotonic() + self.settings.inference_timeout_seconds
        limits = allocate_section_token_limits(self.settings.model_max_output_tokens)
        metrics = GenerationMetrics(output_token_limit=self.settings.model_max_output_tokens)
        bodies = []
        try:
            for section, title, _ in SECTION_SPECS:
                check_deadline(stop, deadline)
                result = self._generate(
                    build_prompt_messages(source, language, section),
                    GenerationOptions(
                        max_output_tokens=limits[section],
                        temperature=self.settings.model_temperature,
                        top_p=self.settings.model_top_p,
                        seed=derive_generation_seed(self.digest, language, section, source),
                    ),
                    deadline,
                    stop,
                )
                observation = SectionMetrics(
                    generated_tokens=result.usage.output_tokens,
                    input_tokens=result.usage.input_tokens,
                    limit_reached=(
                        result.finish_reason == "length"
                        or result.usage.output_tokens >= limits[section]
                    ),
                    generation_ms=result.generation_ms,
                    first_token_ms=result.first_token_ms,
                )
                metrics.sections[section] = observation
                body = normalize_section_body(section, result.content)
                body, observation.trailing_fragment_removed = trim_capped_section_tail(
                    body, observation.limit_reached
                )
                bodies.append(f"## {title}\n{body}")
            return validate_review_output("\n\n".join(bodies), source)
        finally:
            logger.info("model_generation_finished", extra=metrics.fields())
            if self.metrics:
                self.metrics.generation(metrics)

    def close(self):
        self.closing.set()
