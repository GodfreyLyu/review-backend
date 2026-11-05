"""Exercise chunked ASGI transport boundaries independently of TestClient buffering."""

import asyncio
import json

import pytest

from app.api.middleware import RequestGuard


def run_request(app, messages, *, method="POST", headers=None, scope_type="http"):
    scope = {
        "type": scope_type,
        "method": method,
        "headers": headers if headers is not None else [(b"content-type", b"application/json")],
    }
    incoming = iter(messages)
    sent = []

    async def receive():
        return next(incoming)

    async def send(message):
        sent.append(message)

    asyncio.run(RequestGuard(app)(scope, receive, send))
    return scope, sent


@pytest.mark.unit
@pytest.mark.recovery
@pytest.mark.parametrize("size", [0, 131072])
def test_chunked_body_is_replayed_once_then_disconnect_is_forwarded(size):
    body = b"x" * size
    observed = []

    async def app(scope, receive, send):
        observed.extend([await receive(), await receive()])
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    scope, sent = run_request(
        app,
        [
            {"type": "http.request", "body": body[:10], "more_body": True},
            {"type": "http.request", "body": body[10:], "more_body": False},
            {"type": "http.disconnect"},
        ],
    )
    assert observed == [
        {"type": "http.request", "body": body, "more_body": False},
        {"type": "http.disconnect"},
    ]
    assert dict(sent[0]["headers"]) == {
        b"x-request-id": scope["state"]["request_id"].encode(),
        b"cache-control": b"no-store",
        b"x-content-type-options": b"nosniff",
    }


@pytest.mark.unit
@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH"])
def test_chunked_overflow_is_rejected_without_trusting_content_length(method):
    async def app(scope, receive, send):
        pytest.fail("Oversized bodies must not reach the application")

    scope, sent = run_request(
        app,
        [
            {"type": "http.request", "body": b"x" * 131072, "more_body": True},
            {"type": "http.request", "body": b"x", "more_body": True},
        ],
        method=method,
        headers=[(b"content-type", b"application/json"), (b"content-length", b"0")],
    )
    assert sent[0]["status"] == 413
    error = json.loads(sent[1]["body"])["error"]
    assert error["code"] == "input_too_large"
    assert error["request_id"] == scope["state"]["request_id"]


@pytest.mark.unit
def test_disconnect_during_body_read_does_not_invoke_application():
    async def app(scope, receive, send):
        pytest.fail("Disconnected requests must not reach the application")

    _, sent = run_request(
        app,
        [
            {"type": "http.request", "body": b"{", "more_body": True},
            {"type": "http.disconnect"},
        ],
    )
    assert sent == []


@pytest.mark.unit
def test_non_http_scope_passes_through_unchanged():
    async def app(scope, receive, send):
        assert "state" not in scope
        assert await receive() == {"type": "lifespan.startup"}
        await send({"type": "lifespan.startup.complete"})

    _, sent = run_request(app, [{"type": "lifespan.startup"}], scope_type="lifespan")
    assert sent == [{"type": "lifespan.startup.complete"}]
