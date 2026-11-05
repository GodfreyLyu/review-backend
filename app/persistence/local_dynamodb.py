"""DynamoDB Local transport: explicit local endpoint, dummy credentials, no AWS fallback."""

import os
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}
SERVICE_HOSTS = {
    "review-dynamodb",
    "review-dynamodb.local-review-demo.svc",
    "review-dynamodb.local-review-demo.svc.cluster.local",
}


def validate_local_endpoint(endpoint: str, *, loopback_only: bool = False) -> str:
    message = "DYNAMODB_ENDPOINT_URL must be an explicit HTTP DynamoDB Local endpoint with a port."
    try:
        url = urlsplit(endpoint)
        valid = (
            isinstance(endpoint, str)
            and endpoint == endpoint.strip()
            and not any(c.isspace() or ord(c) < 32 for c in endpoint)
            and url.scheme == "http"
            and url.port is not None
            and 1 <= url.port <= 65535
            and url.username is None
            and url.password is None
            and not url.query
            and not url.fragment
            and url.path in {"", "/"}
            and (
                url.hostname in LOOPBACK_HOSTS
                or (not loopback_only and url.hostname in SERVICE_HOSTS and url.port == 8000)
            )
        )
    except (ValueError, TypeError, AttributeError):
        valid = False
    if not valid:
        # Never include the rejected URL; it may contain embedded credentials.
        raise ValueError(message)
    return endpoint


def local_client(endpoint: str, region: str = "ap-northeast-1", *, startup_check: bool = False):
    validate_local_endpoint(endpoint)
    os.environ.update(
        AWS_EC2_METADATA_DISABLED="true",
        AWS_CONFIG_FILE=os.devnull,
        AWS_SHARED_CREDENTIALS_FILE=os.devnull,
    )
    for key in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    ):
        os.environ.pop(key, None)
    # A new session avoids cached host profiles/credentials; the explicit endpoint cannot be
    # replaced by SDK endpoint environment settings. Proxies must not receive local account data.
    session = boto3.Session(
        aws_access_key_id="local", aws_secret_access_key="local", region_name=region
    )
    return session.client(
        "dynamodb",
        endpoint_url=endpoint,
        config=Config(
            connect_timeout=3,
            read_timeout=5,
            # Startup owns its retry budget; ordinary account operations keep SDK retries.
            retries=(
                {"mode": "standard", "total_max_attempts": 1}
                if startup_check
                else {"max_attempts": 2}
            ),
            proxies={},
            ignore_configured_endpoint_urls=True,
        ),
    )
