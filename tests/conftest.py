import boto3
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws

from app.config import Settings
from app.main import create_app
from backend.tests.support import FakeReviewClient, wait_ready


@pytest.fixture
def factory(tmp_path, monkeypatch):
    """Create real API/coordinator lifecycles with Moto accounts and temporary SQLite.

    FakeReviewClient replaces only inference. Closing clients in reverse order drains
    their lifespan workers before the Moto context and temporary storage disappear."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        # Only the test transport uses Moto's intercepted SDK endpoint. Runtime factory safety
        # is tested separately; no application mode allows a regional endpoint.
        monkeypatch.setattr(
            "app.persistence.users.local_client",
            lambda endpoint, region, **kwargs: boto3.client(
                "dynamodb",
                region_name=region,
                aws_access_key_id="testing",
                aws_secret_access_key="testing",
            ),
        )
        boto3.resource("dynamodb", region_name="us-east-1").create_table(
            TableName="test-users",
            KeySchema=[{"AttributeName": "login_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "login_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        clients = []

        def make(review_client=None, ready=True, **overrides):
            values = dict(
                environment="test",
                signing_secret="test-only-" * 5,
                allowed_origin="https://testserver",
                cookie_secure=True,
                data_dir=tmp_path,
                aws_region="us-east-1",
                dynamodb_table="test-users",
                dynamodb_endpoint_url="http://127.0.0.1:8001",
            )
            values.update(overrides)
            if review_client is None:
                review_client = FakeReviewClient()
            app = create_app(Settings(**values), review_client=review_client)
            client = TestClient(
                app, base_url="https://testserver", headers={"Origin": "https://testserver"}
            )
            client.__enter__()
            clients.append(client)
            if ready:
                wait_ready(client)
            return client

        yield make
        for client in reversed(clients):
            client.__exit__(None, None, None)
