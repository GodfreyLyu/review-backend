"""The in-image initializer accepts cluster-local transport and preserves existing data."""

from unittest.mock import Mock

import pytest
from botocore.exceptions import EndpointConnectionError

from app.persistence import initialize_users as init


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.security
def test_initializer_uses_local_endpoint_and_configured_table(monkeypatch):
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "http://review-dynamodb:8000")
    monkeypatch.setenv("DYNAMODB_TABLE", "custom-users")
    client, ensure = Mock(), Mock()
    monkeypatch.setattr(init, "local_client", Mock(return_value=client))
    monkeypatch.setattr(init, "ensure_users_table", ensure)
    init.initialize()
    ensure.assert_called_once_with(client, "custom-users")


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.recovery
def test_initializer_retries_transport_but_not_schema_failure(monkeypatch):
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "http://review-dynamodb:8000")
    monkeypatch.setattr(init, "local_client", Mock())
    monkeypatch.setattr(init.time, "sleep", Mock())
    ensure = Mock(
        side_effect=[EndpointConnectionError(endpoint_url="http://review-dynamodb:8000"), None]
    )
    monkeypatch.setattr(init, "ensure_users_table", ensure)
    init.initialize()
    assert ensure.call_count == 2
    ensure.side_effect = ValueError("schema mismatch")
    with pytest.raises(ValueError, match="schema"):
        init.initialize()
    assert ensure.call_count == 3


@pytest.mark.unit
@pytest.mark.security
def test_initializer_refuses_cloud_endpoint_before_client_creation(monkeypatch):
    monkeypatch.setenv("DYNAMODB_ENDPOINT_URL", "https://dynamodb.us-east-1.amazonaws.com")
    client = Mock()
    monkeypatch.setattr(init, "local_client", client)
    with pytest.raises(ValueError):
        init.initialize()
    client.assert_not_called()
