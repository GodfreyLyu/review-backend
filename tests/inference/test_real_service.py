"""Opt-in API acceptance against an already running inference service."""

import os
import time

import pytest

from app.inference.rpc.client import ThriftReviewClient
from app.inference.settings import InferenceSettings
from backend.tests.support import register


@pytest.mark.real_model
@pytest.mark.integration
def test_real_fastapi_review_persists_remote_identity(factory, record_property):
    if os.environ.get("RUN_REAL_MODEL") != "1":
        pytest.skip("Set RUN_REAL_MODEL=1 and start the inference service first")
    review_client = ThriftReviewClient(
        InferenceSettings(thrift_host=os.environ.get("THRIFT_HOST", "127.0.0.1"))
    )
    client = factory(review_client)
    register(client)
    response = client.post(
        "/api/v1/reviews",
        json={
            "source_code": "def average(values):\n    return sum(values) / len(values)\n",
            "language": "python",
        },
    )
    assert response.status_code == 202
    review_id = response.json()["review_id"]
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        result = client.get(f"/api/v1/reviews/{review_id}").json()
        if result["status"] in {"completed", "failed"}:
            break
        time.sleep(0.2)
    assert result["status"] == "completed", result.get("error_code")
    assert all(
        f"## {section}" in result["review_result"]
        for section in ("Summary", "Findings", "Suggestions")
    )
    assert result["model_revision"] == review_client.digest
    assert client.get("/api/v1/runtime").json()["inference_mode"] == "real"
    record_property("model_revision", review_client.digest)
    record_property("device", review_client.device)
