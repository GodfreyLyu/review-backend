import time
from typing import Protocol, cast
from uuid import uuid4

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError

from app.config import Settings
from app.domain import UserRecord
from app.errors import AppError
from app.persistence.local_dynamodb import local_client


class UserStore(Protocol):
    def ping(self) -> None: ...
    def get(self, login_id: str) -> UserRecord | None: ...
    def create(self, login_id: str, password_hash: str) -> UserRecord: ...


class DynamoUsers:
    def __init__(self, settings: Settings) -> None:
        # Low-level clients can be shared by FastAPI worker threads; resource objects cannot.
        self.client = local_client(settings.dynamodb_endpoint_url, settings.aws_region)
        self._startup_client = local_client(
            settings.dynamodb_endpoint_url, settings.aws_region, startup_check=True
        )
        self.table_name = settings.dynamodb_table

    def ping(self) -> None:
        # Injected harness subclasses own their transport and may only supply `client`.
        client = getattr(self, "_startup_client", self.client)
        table = client.describe_table(TableName=self.table_name)["Table"]
        if (
            table["TableStatus"] != "ACTIVE"
            or table["KeySchema"] != [{"AttributeName": "login_id", "KeyType": "HASH"}]
            or table["AttributeDefinitions"]
            != [{"AttributeName": "login_id", "AttributeType": "S"}]
        ):
            raise ValueError("Local users table schema/status mismatch; existing data retained.")

    def get(self, login_id: str) -> UserRecord | None:
        item = self.client.get_item(
            TableName=self.table_name,
            Key={"login_id": {"S": login_id}},
            ConsistentRead=True,
        ).get("Item")
        if item is None:
            return None
        deserialize = TypeDeserializer().deserialize
        return cast(UserRecord, {key: deserialize(value) for key, value in item.items()})

    def create(self, login_id: str, password_hash: str) -> UserRecord:
        user: UserRecord = {
            "login_id": login_id,
            "user_id": str(uuid4()),
            "password_hash": password_hash,
            "created_at": str(time.time()),
            "disabled": False,
        }
        serialize = TypeSerializer().serialize
        try:
            self.client.put_item(
                TableName=self.table_name,
                Item={key: serialize(value) for key, value in user.items()},
                ConditionExpression="attribute_not_exists(login_id)",
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise AppError(
                    "login_unavailable", "This login identifier is unavailable.", 409
                ) from None
            raise
        return user
