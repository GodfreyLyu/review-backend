"""Kafka client construction; credentials remain configuration, never payloads."""

from confluent_kafka import Consumer, Producer


def kafka_config(settings):
    result = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "security.protocol": settings.kafka_security_protocol,
        "socket.timeout.ms": 5000,
        "socket.connection.setup.timeout.ms": 5000,
    }
    if settings.kafka_sasl_username is not None:
        result["sasl.username"] = settings.kafka_sasl_username
    if settings.kafka_sasl_password is not None:
        result["sasl.password"] = settings.kafka_sasl_password.get_secret_value()
    if "SASL" in settings.kafka_security_protocol:
        result["sasl.mechanism"] = settings.kafka_sasl_mechanism
    if settings.kafka_ssl_ca_location:
        result["ssl.ca.location"] = settings.kafka_ssl_ca_location
    for field, option in (
        ("kafka_ssl_certificate_location", "ssl.certificate.location"),
        ("kafka_ssl_key_location", "ssl.key.location"),
    ):
        if getattr(settings, field, None):
            result[option] = getattr(settings, field)
    return result


def producer(settings):
    return Producer(
        {
            **kafka_config(settings),
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 10000,
            "queue.buffering.max.messages": 100,
        }
    )


def consumer(settings):
    return Consumer(
        {
            **kafka_config(settings),
            "group.id": settings.kafka_group_id,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 10000,
            "queued.max.messages.kbytes": 1024,
            "fetch.message.max.bytes": 65536,
        }
    )
