import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.messaging.events import InvalidEvent, decode_job
from app.messaging.kafka import kafka_config
from app.relay.main import Relay

pytestmark = pytest.mark.unit


def envelope():
    return dict(
        event_id=str(uuid4()),
        review_id=str(uuid4()),
        trace_id=str(uuid4()),
        schema_version=1,
        execution_version="review-v1",
        attempt=1,
        created_at=123.0,
    )


def test_event_contains_only_references_and_rejects_wrong_key_or_business_content():
    data = envelope()
    assert decode_job(json.dumps(data).encode(), data["review_id"].encode()).attempt == 1
    with pytest.raises(InvalidEvent, match="invalid_key"):
        decode_job(json.dumps(data).encode(), b"other")
    data["source_code"] = "private source"
    with pytest.raises(InvalidEvent, match="unexpected_fields"):
        decode_job(json.dumps(data).encode(), data["review_id"].encode())


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt", True),
        ("attempt", 0),
        ("schema_version", 2),
        ("execution_version", "new"),
        ("created_at", float("nan")),
    ],
)
def test_invalid_schema_and_execution_version_are_poison(field, value):
    data = envelope()
    data[field] = value
    with pytest.raises(InvalidEvent):
        decode_job(json.dumps(data).encode(), data["review_id"].encode())


class RelayStore:
    def __init__(self):
        self.sent = []

    def mark_outbox_sent(self, event_id, owner):
        self.sent.append(event_id)
        return True


class Producer:
    def __init__(self, error=None):
        self.error = error
        self.published = []

    def produce(self, topic, key, value, on_delivery):
        self.published.append((topic, key, value))
        self.callback = on_delivery

    def poll(self, timeout):
        callback, self.callback = self.callback, None
        callback(self.error, None)


def test_relay_marks_sent_only_after_broker_confirmation():
    store, kafka = RelayStore(), Producer()
    relay = Relay(store, SimpleNamespace(environment="test"), kafka)
    data = envelope()
    row = dict(
        event_id=data["event_id"], review_id=data["review_id"], topic="review.jobs.v1", payload=data
    )
    assert relay.publish(row)
    assert store.sent == [data["event_id"]]
    assert kafka.published[0][1] == data["review_id"].encode()


def test_failed_delivery_does_not_mark_sent():
    store, kafka = RelayStore(), Producer(error=RuntimeError("unavailable"))
    relay = Relay(store, SimpleNamespace(environment="test"), kafka)
    data = envelope()
    with pytest.raises(RuntimeError, match="unconfirmed"):
        relay.publish(
            dict(
                event_id=data["event_id"],
                review_id=data["review_id"],
                topic="review.jobs.v1",
                payload=data,
            )
        )
    assert store.sent == []


def test_poison_dlq_without_review_id_uses_event_id_as_kafka_key():
    store, kafka = RelayStore(), Producer()
    relay = Relay(store, SimpleNamespace(environment="test"), kafka)
    event_id = str(uuid4())
    assert relay.publish(
        dict(
            event_id=event_id,
            review_id=None,
            topic="review.jobs.dlq.v1",
            payload={"event_id": event_id, "reason": "invalid_key"},
        )
    )
    assert kafka.published[0][1] == event_id.encode()


def test_kafka_security_configuration_supports_sasl_tls():
    settings = SimpleNamespace(
        kafka_bootstrap_servers="broker:9093",
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_username="worker",
        kafka_sasl_password=SimpleNamespace(get_secret_value=lambda: "secret"),
        kafka_sasl_mechanism="SCRAM-SHA-512",
        kafka_ssl_ca_location="/run/ca.pem",
    )
    config = kafka_config(settings)
    assert config["security.protocol"] == "SASL_SSL"
    assert config["ssl.ca.location"] == "/run/ca.pem"
