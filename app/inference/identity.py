"""Public model identity, derived from the actual adapter rather than deployment mode."""

from typing import Literal, NotRequired, TypedDict

from app.inference.client import ReviewClient
from app.inference.rpc.client import ThriftReviewClient


class ModelIdentity(TypedDict):
    inference_mode: Literal["real", "simulated", "unknown"]
    model_id: str | None
    model_revision: str | None
    model_source: Literal["ollama_api", "test_fixture", "unknown"]
    device: Literal["cpu", "gpu", "mixed"] | None
    inference_backend: NotRequired[str]
    quantization: NotRequired[str | None]


def model_identity(review_client: ReviewClient) -> ModelIdentity:
    if isinstance(review_client, ThriftReviewClient):
        info = review_client.info
        return {
            "inference_mode": "real",
            "model_id": info.model_id if info else None,
            "model_revision": info.model_revision if info else None,
            "model_source": "ollama_api",
            "device": info.device if info else None,
            "inference_backend": "ollama",
            "quantization": info.quantization if info else None,
        }
    if getattr(review_client, "simulated", False) is True:
        return {
            "inference_mode": "simulated",
            "model_id": "Simulated model",
            "model_revision": "fixture-v1",
            "model_source": "test_fixture",
            "device": None,
        }
    return {
        "inference_mode": "unknown",
        "model_id": None,
        "model_revision": None,
        "model_source": "unknown",
        "device": None,
    }
