"""Idempotent DynamoDB Local initialization for the Helm backend init container."""

import os
import sys
import time

from app.persistence.local_dynamodb import local_client, validate_local_endpoint
from app.persistence.users_startup import TRANSIENT_CONNECTION_ERRORS


def ensure_users_table(client, name: str) -> None:
    try:
        table = client.describe_table(TableName=name)["Table"]
    except client.exceptions.ResourceNotFoundException:
        try:
            client.create_table(
                TableName=name,
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[{"AttributeName": "login_id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "login_id", "AttributeType": "S"}],
            )
        except client.exceptions.ResourceInUseException:
            pass
        client.get_waiter("table_exists").wait(
            TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30}
        )
        table = client.describe_table(TableName=name)["Table"]
    if (
        table["TableStatus"] != "ACTIVE"
        or table["KeySchema"] != [{"AttributeName": "login_id", "KeyType": "HASH"}]
        or table["AttributeDefinitions"] != [{"AttributeName": "login_id", "AttributeType": "S"}]
    ):
        raise ValueError(
            "DynamoDB Local users table schema/status mismatch; existing data retained."
        )


def initialize() -> None:
    endpoint = os.environ.get("DYNAMODB_ENDPOINT_URL", "")
    validate_local_endpoint(endpoint)
    client = local_client(
        endpoint, os.environ.get("AWS_REGION", "ap-northeast-1"), startup_check=True
    )
    deadline = time.monotonic() + 120
    while True:
        try:
            ensure_users_table(client, os.environ.get("DYNAMODB_TABLE", "llm-review-users"))
            return
        except TRANSIENT_CONNECTION_ERRORS:
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(2, max(0, deadline - time.monotonic())))


if __name__ == "__main__":
    try:
        initialize()
    except Exception as error:
        print(f"Local users initialization failed ({type(error).__name__}).", file=sys.stderr)
        sys.exit(1)
