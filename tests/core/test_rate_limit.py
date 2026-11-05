"""Observable quota behavior, independent of HTTP and account hashing."""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import AppError
from app.rate_limit import RateLimiter


@pytest.mark.unit
def test_window_boundary_and_rejections_do_not_extend_the_quota(monkeypatch):
    now = 0.0
    monkeypatch.setattr("app.rate_limit.time.monotonic", lambda: now)
    limiter = RateLimiter()
    limiter.check("account", 2)
    now = 10.0
    limiter.check("account", 2)
    now = 59.999
    with pytest.raises(AppError) as failure:
        limiter.check("account", 2)
    assert failure.value.code == "rate_limited"
    assert failure.value.status == 429
    assert failure.value.message == "Too many requests. Try again in one minute."
    now = 60.0
    limiter.check("account", 2)
    now = 69.999
    with pytest.raises(AppError):
        limiter.check("account", 2)
    now = 70.0
    limiter.check("account", 2)


@pytest.mark.unit
def test_custom_window_and_independent_keys_and_instances(monkeypatch):
    now = 0.0
    monkeypatch.setattr("app.rate_limit.time.monotonic", lambda: now)
    limiter = RateLimiter()
    limiter.check("alice", 1, window=5)
    limiter.check("bob", 1, window=5)
    RateLimiter().check("alice", 1, window=5)
    now = 4.999
    with pytest.raises(AppError):
        limiter.check("alice", 1, window=5)
    now = 5.0
    limiter.check("alice", 1, window=5)


@pytest.mark.unit
def test_concurrent_requests_cannot_exceed_one_keys_quota():
    limiter = RateLimiter()
    barrier = threading.Barrier(8)

    def attempt(_):
        barrier.wait()
        try:
            limiter.check("shared", 3)
            return "accepted"
        except AppError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(attempt, range(8)))
    assert outcomes.count("accepted") == 3
    assert outcomes.count("rate_limited") == 5
