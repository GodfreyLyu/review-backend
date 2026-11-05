from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict

from app.inference.settings import InferenceSettings
from app.persistence.local_dynamodb import validate_local_endpoint


class Settings(InferenceSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    environment: Literal["local", "test"] = "local"
    deployment_environment: Literal["minikube", "development", "unknown"] = "unknown"
    enable_api_docs: bool = False
    release_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{7,64}$")
    signing_secret: SecretStr
    allowed_origin: str = "http://localhost:5173"
    cookie_secure: bool = True
    session_ttl_seconds: int = Field(default=1800, ge=60, le=3600)
    data_dir: Path = Path("/data")
    aws_region: str = "ap-northeast-1"
    dynamodb_table: str = "llm-review-users"
    dynamodb_endpoint_url: str
    inference_drain_seconds: float = Field(default=30, gt=0, le=120)
    shutdown_grace_seconds: float = Field(default=20, gt=0, le=60)
    queue_capacity: int = Field(default=8, ge=1, le=32)
    max_retries: int = Field(default=1, ge=0, le=2)
    login_rate_limit: int = Field(default=10, ge=1, le=100)
    submission_rate_limit: int = Field(default=6, ge=1, le=60)

    @model_validator(mode="after")
    def secure_configuration(self):
        if len(self.signing_secret.get_secret_value()) < 32:
            raise ValueError("SIGNING_SECRET must contain at least 32 characters.")
        if self.allowed_origin.endswith("/"):
            raise ValueError("ALLOWED_ORIGIN must not end with a slash.")
        validate_local_endpoint(self.dynamodb_endpoint_url)
        return self
