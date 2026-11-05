"""Backend connection settings; no native model runtime configuration."""

import ipaddress
import re

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.model_config import ModelSettings


def local_host(host: str) -> bool:
    if host in {"localhost", "review-ollama", "host.docker.internal", "host.minikube.internal"}:
        return True
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if re.fullmatch(rf"{label}\.{label}\.svc\.cluster\.local", host):
        return True
    try:
        address = ipaddress.ip_address(host)
        return address.is_loopback or any(
            address in ipaddress.ip_network(network)
            for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
        )
    except ValueError:
        return False


class InferenceSettings(ModelSettings, BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    thrift_host: str = "review-ollama.local-inference.svc.cluster.local"
    thrift_port: int = Field(default=9091, ge=1, le=65535)
    thrift_socket_timeout_seconds: float = Field(default=2, gt=0, le=10)
    thrift_recovery_seconds: float = Field(default=20, gt=0, le=60)
    source_max_chars: int = Field(default=12000, ge=1, le=32000)

    @model_validator(mode="after")
    def local_thrift(self):
        if not local_host(self.thrift_host):
            raise ValueError("THRIFT_HOST must identify a local inference service.")
        return self
