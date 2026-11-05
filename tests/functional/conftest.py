"""Dedicated PostgreSQL test database only; never migrate or truncate application data."""

import os
from urllib.parse import urlsplit

import pytest

from app.config import Settings
from app.persistence.storage import Store


@pytest.fixture
def database_url():
    url = os.getenv("BACKEND_TEST_DATABASE_URL")
    if not url:
        pytest.skip("BACKEND_TEST_DATABASE_URL is required for PostgreSQL functional tests")
    name = urlsplit(url).path.rsplit("/", 1)[-1]
    if "test" not in name.lower():
        pytest.fail("Functional database name must contain 'test'; refusing destructive setup")
    return url


@pytest.fixture
def database(database_url):
    return database_url


@pytest.fixture
def store(database_url):
    result = Store(database_url)
    result.initialize()
    with result.connection(write=True) as db:
        db.execute(
            "TRUNCATE outbox,kafka_dlq,rate_limits,sessions,inference_slots,reviews,users CASCADE"
        )
        db.execute("INSERT INTO inference_slots VALUES ('default',NULL)")
    yield result


@pytest.fixture
def settings(database_url):
    return Settings(
        environment="test",
        database_url=database_url,
        signing_secret="functional-test-secret-" * 3,
        allowed_origin="http://testserver",
        cookie_secure=False,
        kafka_bootstrap_servers=os.getenv(
            "BACKEND_TEST_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:59092"
        ),
        thrift_host="127.0.0.1",
        queue_capacity=8,
        login_rate_limit=100,
        submission_rate_limit=60,
        worker_lease_seconds=3,
    )
