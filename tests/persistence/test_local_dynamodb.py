"""The runtime can only construct a local client, even under hostile host SDK settings."""

import json
import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.awsrequest import AWSResponse
from pydantic import ValidationError

from app.config import Settings
from app.persistence.local_dynamodb import local_client, validate_local_endpoint
from app.persistence.users import DynamoUsers


@pytest.mark.component
@pytest.mark.security
@pytest.mark.parametrize(
    "endpoint",
    [
        None,
        "",
        "https://dynamodb.us-east-1.amazonaws.com",
        "http://dynamodb.us-east-1.amazonaws.com:80",
        "http://169.254.169.254:80",
        "http://169.254.170.23:80",
        "http://192.168.1.1:8000",
        "http://localhost.evil:8000",
        "http://localhost",
        "http://localhost:0",
        "http://localhost:65536",
        "http://localhost:8001/table",
        "http://localhost:8001?x=1",
        "http://localhost:8001#private",
        "http://PRIVATE:SECRET@localhost:8001",
        " http://localhost:8001",
        "http://local\nhost:8001",
        "http://review-dynamodb:80",
        "https://localhost:8001",
    ],
)
def test_nonlocal_missing_or_ambiguous_endpoints_fail_before_sdk(endpoint, monkeypatch):
    session = Mock(side_effect=AssertionError("No SDK initialization permitted"))
    monkeypatch.setattr("app.persistence.local_dynamodb.boto3.Session", session)
    with pytest.raises(ValueError) as error:
        local_client(endpoint)
    assert "PRIVATE" not in str(error.value) and "SECRET" not in str(error.value)
    session.assert_not_called()


@pytest.mark.component
@pytest.mark.security
@pytest.mark.parametrize(
    "endpoint",
    [
        "http://localhost:8001",
        "http://127.0.0.1:30001/",
        "http://[::1]:8001",
        "http://review-dynamodb:8000",
        "http://review-dynamodb.local-review-demo.svc:8000",
        "http://review-dynamodb.local-review-demo.svc.cluster.local:8000",
    ],
)
def test_explicit_local_endpoints_are_valid(endpoint):
    assert validate_local_endpoint(endpoint) == endpoint


@pytest.mark.component
@pytest.mark.security
def test_settings_require_local_endpoint_even_in_test_mode(monkeypatch):
    monkeypatch.delenv("DYNAMODB_ENDPOINT_URL", raising=False)
    for values in ({}, {"dynamodb_endpoint_url": "https://dynamodb.us-east-1.amazonaws.com"}):
        with pytest.raises(ValidationError):
            Settings(
                _env_file=None, signing_secret="private-secret-" * 4, environment="test", **values
            )


@pytest.mark.component
@pytest.mark.security
def test_client_ignores_host_credentials_profiles_metadata_and_endpoint_overrides(
    tmp_path, monkeypatch
):
    for key, value in {
        "AWS_PROFILE": "do-not-load",
        "AWS_DEFAULT_PROFILE": "do-not-load",
        "AWS_CONFIG_FILE": str(tmp_path / "never-read"),
        "AWS_SHARED_CREDENTIALS_FILE": str(tmp_path / "never-read"),
        "AWS_ACCESS_KEY_ID": "PRIVATE_HOST_KEY",
        "AWS_SECRET_ACCESS_KEY": "PRIVATE_HOST_SECRET",
        "AWS_SESSION_TOKEN": "PRIVATE_TOKEN",
        "AWS_EC2_METADATA_DISABLED": "false",
        "AWS_WEB_IDENTITY_TOKEN_FILE": str(tmp_path / "never-read"),
        "AWS_ROLE_ARN": "PRIVATE_ROLE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://169.254.170.2/credentials",
        "AWS_ENDPOINT_URL": "https://dynamodb.us-east-1.amazonaws.com",
        "AWS_ENDPOINT_URL_DYNAMODB": "https://dynamodb.us-east-1.amazonaws.com",
        "HTTP_PROXY": "http://proxy.invalid:8080",
    }.items():
        monkeypatch.setenv(key, value)
    # No network is needed for construction. Any attempt to fetch credentials fails the test.
    monkeypatch.setattr(
        "botocore.httpsession.URLLib3Session.send",
        Mock(side_effect=AssertionError("Unexpected network")),
    )
    settings = Settings(
        _env_file=None,
        signing_secret="local-test-" * 4,
        dynamodb_endpoint_url="http://127.0.0.1:8001",
    )
    users = DynamoUsers(settings)
    client = users.client
    credentials = client._request_signer._credentials.get_frozen_credentials()
    assert (credentials.access_key, credentials.secret_key, credentials.token) == (
        "local",
        "local",
        None,
    )
    assert client.meta.endpoint_url == "http://127.0.0.1:8001"
    assert client.meta.config.proxies == {}
    assert os.environ["AWS_EC2_METADATA_DISABLED"] == "true"
    assert os.environ["AWS_CONFIG_FILE"] == os.environ["AWS_SHARED_CREDENTIALS_FILE"] == os.devnull
    assert "AWS_PROFILE" not in os.environ and "AWS_SESSION_TOKEN" not in os.environ
    sent = []

    def offline_send(_session, request):
        assert request.url == "http://127.0.0.1:8001/"
        assert b"Credential=local/" in request.headers["Authorization"]
        assert "PRIVATE" not in str(request.headers)
        sent.append(request.url)
        return AWSResponse(
            request.url,
            200,
            {"content-type": "application/x-amz-json-1.0"},
            SimpleNamespace(
                stream=lambda: iter(
                    [
                        json.dumps(
                            {
                                "Table": {
                                    "TableStatus": "ACTIVE",
                                    "KeySchema": [{"AttributeName": "login_id", "KeyType": "HASH"}],
                                    "AttributeDefinitions": [
                                        {"AttributeName": "login_id", "AttributeType": "S"}
                                    ],
                                }
                            }
                        ).encode()
                    ]
                )
            ),
        )

    monkeypatch.setattr("botocore.httpsession.URLLib3Session.send", offline_send)
    users.ping()  # Exercise the actual serializer/signing/transport choice; no network is used.
    assert sent == ["http://127.0.0.1:8001/"]


@pytest.mark.component
@pytest.mark.recovery
def test_startup_transport_has_one_attempt_even_with_sdk_retry_environment(monkeypatch):
    from botocore.exceptions import ConnectTimeoutError

    monkeypatch.setenv("AWS_MAX_ATTEMPTS", "99")
    monkeypatch.setenv("AWS_RETRY_MODE", "adaptive")
    settings = Settings(
        _env_file=None,
        signing_secret="local-test-" * 4,
        dynamodb_endpoint_url="http://127.0.0.1:8001",
    )
    users = DynamoUsers(settings)
    assert users.client.meta.config.retries["total_max_attempts"] == 3
    startup = users._startup_client.meta.config
    assert startup.retries == {"mode": "standard", "total_max_attempts": 1}
    assert (startup.connect_timeout, startup.read_timeout) == (3, 5)
    assert startup.proxies == {}
    send = Mock(side_effect=ConnectTimeoutError(endpoint_url="PRIVATE"))
    monkeypatch.setattr("botocore.httpsession.URLLib3Session.send", send)
    try:
        with pytest.raises(ConnectTimeoutError):
            users.ping()
        send.assert_called_once()  # Actual botocore pipeline; no hidden SDK retry or real network.
    finally:
        users.client.close()
        users._startup_client.close()


@pytest.mark.component
@pytest.mark.parametrize(
    "change",
    [
        {"TableStatus": "CREATING"},
        {"KeySchema": [{"AttributeName": "wrong", "KeyType": "HASH"}]},
        {"AttributeDefinitions": [{"AttributeName": "login_id", "AttributeType": "N"}]},
    ],
)
def test_startup_ping_rejects_invalid_table_without_writing(change):
    users = DynamoUsers.__new__(DynamoUsers)
    users.table_name = "test-users"
    users.client = Mock()
    users._startup_client = Mock()
    users._startup_client.describe_table.return_value = {
        "Table": {
            "TableStatus": "ACTIVE",
            "KeySchema": [{"AttributeName": "login_id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "login_id", "AttributeType": "S"}],
            **change,
        }
    }
    with pytest.raises(ValueError, match="schema/status mismatch"):
        users.ping()
    assert len(users._startup_client.mock_calls) == 1
    users._startup_client.describe_table.assert_called_once_with(TableName="test-users")
    assert users.client.mock_calls == []
