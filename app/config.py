"""Process configuration. Secrets and submitted content are never formatted in errors."""

from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import SettingsConfigDict

from app.inference.settings import InferenceSettings


class Settings(InferenceSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    environment: Literal["local", "test", "production"] = "local"
    deployment_environment: Literal["minikube", "development", "production", "unknown"] = "unknown"
    enable_api_docs: bool = False
    release_sha: str | None = Field(default=None, pattern=r"^[0-9a-f]{7,64}$")
    signing_secret: SecretStr
    database_url: str = "postgresql://review:review@127.0.0.1:55432/review_backend"
    allowed_origin: str = "http://localhost:5173"
    cookie_secure: bool = True
    session_ttl_seconds: int = Field(default=1800, ge=60, le=3600)
    queue_capacity: int = Field(default=8, ge=1, le=10000)
    max_active_per_user: int = Field(default=1, ge=1, le=10)
    max_retries: int = Field(default=2, ge=0, le=10)
    max_recovery_attempts: int = Field(default=8, ge=1, le=100)
    login_rate_limit: int = Field(default=10, ge=1, le=100)
    submission_rate_limit: int = Field(default=6, ge=1, le=60)
    task_deadline_seconds: float = Field(default=600, gt=0, le=86400)
    shutdown_grace_seconds: float = Field(default=20, gt=0, le=120)
    kafka_bootstrap_servers: str = "127.0.0.1:59092"
    kafka_topic: str = "review.jobs.v1"
    kafka_dlq_topic: str = "review.jobs.dlq.v1"
    kafka_group_id: str = "review-worker-v1"
    kafka_security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    kafka_ssl_ca_location: str | None = None
    kafka_ssl_certificate_location: str | None = None
    kafka_ssl_key_location: str | None = None
    worker_lease_seconds: float = Field(default=30, ge=1, le=600)
    worker_poll_seconds: float = Field(default=0.2, gt=0, le=5)
    retry_base_seconds: float = Field(default=1, gt=0, le=60)
    retry_max_seconds: float = Field(default=30, gt=0, le=600)
    start_watchdog_seconds: float = Field(default=60, gt=0, le=3600)
    relay_lease_seconds: float = Field(default=30, ge=1, le=600)
    relay_batch_size: int = Field(default=10, ge=1, le=100)

    @model_validator(mode="after")
    def secure_configuration(self):
        if len(self.signing_secret.get_secret_value()) < 32:
            raise ValueError("SIGNING_SECRET must contain at least 32 characters.")
        if self.allowed_origin.endswith("/"):
            raise ValueError("ALLOWED_ORIGIN must not end with a slash.")
        if not self.database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection URL.")
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("RETRY_MAX_SECONDS must be at least RETRY_BASE_SECONDS.")
        if self.environment == "production" and (
            not self.cookie_secure
            or self.kafka_security_protocol in {"PLAINTEXT", "SASL_PLAINTEXT"}
        ):
            raise ValueError("Production requires secure cookies and encrypted Kafka transport.")
        return self
