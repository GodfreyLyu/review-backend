import time

import boto3
import pytest
from argon2 import extract_parameters
from argon2.low_level import Type
from fastapi.testclient import TestClient

from backend.tests.support import FakeReviewClient, register, wait_review


@pytest.mark.component
@pytest.mark.security
def test_registration_hashing_cookie_and_normalization(factory):
    client = factory()
    response = register(client, " Alice ")
    assert response.json()["login_id"] == "alice"
    assert all(
        flag in response.headers["set-cookie"]
        for flag in ["__Host-review_session", "HttpOnly", "Secure", "SameSite=strict"]
    )
    user = (
        boto3.resource("dynamodb", region_name="us-east-1")
        .Table("test-users")
        .get_item(Key={"login_id": "alice"})["Item"]
    )
    assert extract_parameters(user["password_hash"]).type is Type.ID
    assert "correct-horse" not in user["password_hash"]
    assert (
        client.post(
            "/api/v1/auth/register", json={"login_id": "ALICE", "password": "correct-horse-battery"}
        ).status_code
        == 409
    )
    assert client.get("/api/v1/auth/me").json()["login_id"] == "alice"


@pytest.mark.component
@pytest.mark.security
@pytest.mark.recovery
def test_logout_revokes_replayed_cookie_and_login(factory):
    client = factory()
    register(client)
    old = client.cookies.get("__Host-review_session")
    assert client.post("/api/v1/auth/logout", json={}).status_code == 204
    client.cookies.set("__Host-review_session", old)
    assert client.get("/api/v1/auth/me").status_code == 401
    client.cookies.clear()
    assert (
        client.post(
            "/api/v1/auth/login", json={"login_id": "alice", "password": "a-wrong-password"}
        ).status_code
        == 401
    )
    assert (
        client.post(
            "/api/v1/auth/login", json={"login_id": "alice", "password": "correct-horse-battery"}
        ).status_code
        == 200
    )


@pytest.mark.component
def test_session_expiry_and_disabled_user(factory):
    client = factory()
    register(client)
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("test-users")
    for disabled, expected in [(True, 401), (False, 200)]:
        table.update_item(
            Key={"login_id": "alice"},
            UpdateExpression="SET disabled = :v",
            ExpressionAttributeValues={":v": disabled},
        )
        assert client.get("/api/v1/auth/me").status_code == expected
    with client.app.state.store.connection(write=True) as db:
        db.execute("UPDATE sessions SET expires_at=?", (time.time() - 1,))
    assert client.get("/api/v1/auth/me").status_code == 401


@pytest.mark.component
@pytest.mark.security
def test_csrf_and_json_only(factory):
    client = factory()
    payload = {"login_id": "alice", "password": "correct-horse-battery"}
    assert (
        client.post(
            "/api/v1/auth/register", json=payload, headers={"Origin": "https://evil.example"}
        ).status_code
        == 403
    )
    register(client)
    for path, body in [("/api/v1/reviews", {"source_code": "x=1"}), ("/api/v1/auth/logout", {})]:
        assert client.post(path, json=body, headers={"X-CSRF-Token": "bad"}).status_code == 403
        assert (
            client.post(path, json=body, headers={"Origin": "https://evil.example"}).status_code
            == 403
        )
    assert (
        client.post(
            "/api/v1/auth/login", content="{}", headers={"content-type": "text/plain"}
        ).status_code
        == 415
    )


@pytest.mark.component
@pytest.mark.contract
@pytest.mark.security
def test_user_isolation_and_model_provenance(factory):
    review_client = FakeReviewClient()
    client = factory(review_client)
    register(client)
    response = client.post("/api/v1/reviews", json={"source_code": "def f(x): return 1/x"})
    assert response.status_code == 202
    review_id = response.json()["review_id"]
    review = wait_review(client, review_id)
    assert review["status"] == "completed"
    assert review["model_id"] == "Simulated model"
    assert review["model_revision"] == "fixture-v1" and "user_id" not in review
    other = TestClient(
        client.app, base_url="https://testserver", headers={"Origin": "https://testserver"}
    )
    register(other, "bob")
    assert other.get(f"/api/v1/reviews/{review_id}").status_code == 404
    assert other.get("/api/v1/reviews").json()["items"] == []
    assert other.get(f"/api/v1/reviews?before={review_id}").status_code == 400
    assert client.get("/api/v1/reviews").json()["items"][0]["review_id"] == review_id
    assert review_client.peak == 1


@pytest.mark.component
def test_submission_and_login_rate_limits(factory):
    client = factory(
        FakeReviewClient(delay=0.4), queue_capacity=1, submission_rate_limit=2, login_rate_limit=2
    )
    register(client)
    assert client.post("/api/v1/reviews", json={"source_code": "x"}).status_code == 202
    assert client.post("/api/v1/reviews", json={"source_code": "y"}).status_code in (409, 429)
    assert client.post("/api/v1/reviews", json={"source_code": "z"}).status_code == 429
    payload = {"login_id": "unknown", "password": "a-wrong-password"}
    assert client.post("/api/v1/auth/login", json=payload).status_code == 401
    limited = client.post("/api/v1/auth/login", json=payload)
    assert limited.status_code == 429 and limited.headers["retry-after"] == "60"


@pytest.mark.component
@pytest.mark.contract
def test_forwarding_headers_cannot_create_new_rate_limit_identity(factory):
    client = factory(login_rate_limit=1)
    register(client)
    response = client.post(
        "/api/v1/auth/register",
        json={"login_id": "another-user", "password": "correct-horse-battery"},
        headers={"CloudFront-Viewer-Address": "192.0.2.10:1234", "X-Forwarded-For": "192.0.2.11"},
    )
    assert response.status_code == 429
