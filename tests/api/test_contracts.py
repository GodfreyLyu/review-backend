"""Public response allowlists and per-application service behavior."""

import pytest

from backend.tests.support import register, wait_review

SESSION_FIELDS = {"login_id", "csrf_token", "expires_at", "source_max_chars"}
ACCEPTED_FIELDS = {"review_id", "status", "client_request_id"}
DETAIL_FIELDS = ACCEPTED_FIELDS | {
    "language",
    "source_code",
    "review_result",
    "error_code",
    "error_message",
    "model_id",
    "model_revision",
    "retry_count",
    "created_at",
    "updated_at",
}
SUMMARY_FIELDS = {"review_id", "language", "status", "error_code", "created_at", "updated_at"}


@pytest.mark.component
@pytest.mark.contract
@pytest.mark.security
def test_authentication_responses_preserve_fields_and_numeric_expiry(factory):
    client = factory()
    created = register(client)
    current = client.get("/api/v1/auth/me")
    assert current.json() == created.json()
    assert set(created.json()) == SESSION_FIELDS
    assert isinstance(created.json()["expires_at"], float)
    assert isinstance(created.json()["source_max_chars"], int)
    assert client.post("/api/v1/auth/logout", json={}).content == b""
    login = client.post(
        "/api/v1/auth/login", json={"login_id": "alice", "password": "correct-horse-battery"}
    )
    assert login.status_code == 200
    assert set(login.json()) == SESSION_FIELDS
    assert login.json()["csrf_token"] != created.json()["csrf_token"]


@pytest.mark.component
@pytest.mark.contract
@pytest.mark.security
def test_review_responses_allowlist_fields_without_mutating_storage_rows(factory, monkeypatch):
    client = factory()
    register(client)
    accepted = client.post("/api/v1/reviews", json={"source_code": "value = 1"})
    assert accepted.status_code == 202
    assert set(accepted.json()) == ACCEPTED_FIELDS
    review_id = accepted.json()["review_id"]
    completed = wait_review(client, review_id)
    assert set(completed) == DETAIL_FIELDS
    assert completed["error_code"] is None and completed["error_message"] is None
    assert isinstance(completed["created_at"], float)
    assert isinstance(completed["updated_at"], float)
    assert isinstance(completed["retry_count"], int)

    store = client.app.state.store
    original_detail, original_history = store.get_review, store.history
    captured = []

    def detail_with_internal_fields(*args):
        row = original_detail(*args) | {"future_internal_field": "PRIVATE", "token_hash": "PRIVATE"}
        captured.append(row)
        return row

    def history_with_internal_fields(*args):
        page = original_history(*args)
        page["internal_cursor"] = "PRIVATE"
        page["items"][0].update(source_code="PRIVATE", user_id="PRIVATE")
        return page

    monkeypatch.setattr(store, "get_review", detail_with_internal_fields)
    monkeypatch.setattr(store, "history", history_with_internal_fields)
    response = client.get(f"/api/v1/reviews/{review_id}")
    assert response.json() == completed
    assert "user_id" in captured[0] and "future_internal_field" in captured[0]
    page = client.get("/api/v1/reviews").json()
    assert set(page) == {"items", "next_cursor"}
    assert page["next_cursor"] is None
    assert set(page["items"][0]) == SUMMARY_FIELDS
    assert "PRIVATE" not in response.text and "PRIVATE" not in str(page)


@pytest.mark.component
def test_submission_rate_limits_belong_to_each_application(factory, tmp_path):
    first = factory(data_dir=tmp_path / "first", submission_rate_limit=1)
    second = factory(data_dir=tmp_path / "second", submission_rate_limit=1)
    register(first)
    login = second.post(
        "/api/v1/auth/login", json={"login_id": "alice", "password": "correct-horse-battery"}
    )
    assert login.status_code == 200
    second.headers["X-CSRF-Token"] = login.json()["csrf_token"]
    payload = {"source_code": "x = 1"}
    first_job = first.post("/api/v1/reviews", json=payload)
    assert first_job.status_code == 202
    wait_review(first, first_job.json()["review_id"])
    assert first.post("/api/v1/reviews", json=payload).status_code == 429
    second_job = second.post("/api/v1/reviews", json=payload)
    assert second_job.status_code == 202
    wait_review(second, second_job.json()["review_id"])
    assert first.get(f"/api/v1/reviews/{second_job.json()['review_id']}").status_code == 404


@pytest.mark.component
def test_api_documentation_requires_explicit_opt_in(factory, tmp_path):
    default = factory(data_dir=tmp_path / "default")
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert default.get(path).status_code == 404
    development = factory(data_dir=tmp_path / "development", enable_api_docs=True)
    assert development.get("/docs").status_code == 200
    assert development.get("/redoc").status_code == 404
    specification = development.get("/openapi.json")
    assert specification.status_code == 200
    schemas = specification.json()["components"]["schemas"]
    assert set(schemas["SessionResponse"]["properties"]) == SESSION_FIELDS
    assert set(schemas["ReviewDetail"]["properties"]) == DETAIL_FIELDS
    assert "user_id" not in schemas["ReviewDetail"]["properties"]
    responses = specification.json()["paths"]["/api/v1/reviews"]["post"]["responses"]
    assert responses["422"]["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ErrorResponse"
    )
    assert default.get("/docs").status_code == 404
