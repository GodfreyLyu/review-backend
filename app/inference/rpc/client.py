"""Short generic Thrift adapter. Review policy and persistence live in application."""

from llm_contract.generated.ttypes import LlmError
from thrift.Thrift import TException

from app.errors import AppError, InferenceUnavailable
from app.inference.rpc.transport import Connection


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
    def __init__(self, settings):
        self.settings = settings
        self.info = None

    def _call(self, method, *args):
        with Connection(self.settings) as client:
            return getattr(client, method)(*args)

    def connect(self):
        try:
            info = self._call("getModelInfo")
            health = self._call("getHealth")
            if not health.ready or health.draining or info.limits.active_generations < 1:
                raise InferenceUnavailable()
            if not info.instance_id or not info.model_id or not info.model_revision:
                raise AppError("inference_failed", "Invalid inference metadata.", 503)
            if (
                self.settings.expected_model_id and info.model_id != self.settings.expected_model_id
            ) or (
                self.settings.expected_model_revision
                and info.model_revision != self.settings.expected_model_revision
            ):
                raise AppError("ollama_model_mismatch", "Unexpected inference model identity.", 503)
        except (TException, OSError, InferenceUnavailable):
            self.info = None
            raise InferenceUnavailable() from None
        self.info = info
        return info

    def start(self, request):
        return self._call("startGeneration", request)

    def get(self, instance_id, request_id):
        return self._call("getGeneration", instance_id, request_id)

    def cancel(self, instance_id, request_id):
        return self._call("cancelGeneration", instance_id, request_id)
