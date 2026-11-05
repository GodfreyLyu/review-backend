import pytest
from pydantic import ValidationError

from app.config import Settings


@pytest.mark.unit
@pytest.mark.contract
def test_release_sha_is_optional_but_must_be_a_git_style_hex_digest():
    base = {"signing_secret": "test-secret-" * 4, "dynamodb_endpoint_url": "http://127.0.0.1:8001"}
    assert Settings(**base, release_sha="abcdef012345").release_sha == "abcdef012345"
    with pytest.raises(ValidationError):
        Settings(**base, release_sha="not-a-release")


@pytest.mark.unit
@pytest.mark.security
def test_removed_cloud_environment_is_rejected_without_leaking_signing_secret():
    secret = "this-secret-must-not-appear-in-startup-logs"
    with pytest.raises(ValidationError) as error:
        Settings(
            environment="production",
            cookie_secure=False,
            signing_secret=secret,
            dynamodb_endpoint_url="http://127.0.0.1:8001",
        )
    assert secret not in str(error.value)


@pytest.mark.unit
@pytest.mark.contract
def test_model_selection_and_sampling_are_configurable_from_environment(monkeypatch):
    monkeypatch.setenv("EXPECTED_MODEL_ID", "llama3.2:1b")
    monkeypatch.setenv("MODEL_TEMPERATURE", "0.1")
    config = Settings(dynamodb_endpoint_url="http://localhost:8001", signing_secret="x" * 32)
    assert config.expected_model_id == "llama3.2:1b"
    assert config.model_temperature == 0.1


@pytest.mark.unit
@pytest.mark.contract
@pytest.mark.parametrize(
    "overrides",
    [
        {"model_max_output_tokens": 0},
        {"model_top_p": 0},
        {"model_temperature": -1},
        {"thrift_socket_timeout_seconds": 0},
        {"expected_model_revision": "latest"},
    ],
)
def test_invalid_model_configuration_is_rejected(overrides):
    with pytest.raises(ValidationError):
        Settings(
            dynamodb_endpoint_url="http://localhost:8001", signing_secret="x" * 32, **overrides
        )


@pytest.mark.unit
@pytest.mark.parametrize("output_tokens", [1, 2])
def test_output_budget_must_allow_all_three_sections(output_tokens):
    with pytest.raises(ValidationError):
        Settings(
            dynamodb_endpoint_url="http://127.0.0.1:8001",
            signing_secret="test-secret-" * 4,
            model_max_output_tokens=output_tokens,
        )
