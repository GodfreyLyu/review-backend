"""PostgreSQL polling outbox relay; at-least-once confirmed publication."""

import json
import logging
import os
import signal
import threading
import time
from pathlib import Path
from uuid import uuid4

from app.config import Settings
from app.fault_injection import pause
from app.messaging.kafka import producer
from app.persistence.storage import Store

logger = logging.getLogger("review.relay")


class Relay:
    def __init__(self, store, settings, kafka=None):
        self.store, self.settings = store, settings
        self.kafka = kafka if kafka is not None else producer(settings)
        self.owner = str(uuid4())

    def publish(self, row):
        delivered = threading.Event()
        errors = []

        def confirmed(error, _message):
            errors.append(error)
            delivered.set()

        self.kafka.produce(
            row["topic"],
            key=(row.get("review_id") or row["event_id"]).encode(),
            value=json.dumps(row["payload"], separators=(",", ":")).encode(),
            on_delivery=confirmed,
        )
        deadline = time.monotonic() + 12
        while not delivered.is_set() and time.monotonic() < deadline:
            self.kafka.poll(0.1)
            Path(os.environ.get("PROCESS_HEARTBEAT_FILE", "/tmp/outbox-relay.heartbeat")).touch()
        if not delivered.is_set() or errors[0] is not None:
            raise RuntimeError("kafka_delivery_unconfirmed")
        pause(self.settings, "REVIEW_TEST_PAUSE_AFTER_PUBLISH_FILE", getattr(self, "stop", None))
        # Crashing here duplicates publication on lease expiry. The event ID is unchanged.
        return self.store.mark_outbox_sent(row["event_id"], self.owner)

    def pump(self):
        self.store.recover_due(
            max_attempts=self.settings.max_retries + 1,
            delay=self.settings.retry_base_seconds,
            max_start_seconds=self.settings.start_watchdog_seconds,
            max_recovery_attempts=self.settings.max_recovery_attempts,
        )
        sent = 0
        for _ in range(self.settings.relay_batch_size):
            # Claim one at a time so queued rows cannot expire while another send waits.
            rows = self.store.claim_outbox(self.owner, self.settings.relay_lease_seconds, 1)
            if not rows:
                break
            row = rows[0]
            try:
                if self.publish(row):
                    sent += 1
            except Exception as exc:
                logger.warning(
                    "outbox_delivery_retry",
                    extra={
                        "event_id": row["event_id"],
                        "exception_type": type(exc).__name__,
                    },
                )
                self.store.release_outbox(
                    row["event_id"], self.owner, self.settings.retry_base_seconds
                )
                break
        return sent

    def run(self, stop):
        self.stop = stop
        try:
            while not stop.is_set():
                try:
                    self.pump()
                    Path(
                        os.environ.get("PROCESS_HEARTBEAT_FILE", "/tmp/outbox-relay.heartbeat")
                    ).touch()
                except Exception as exc:
                    logger.warning(
                        "relay_dependency_unavailable",
                        extra={
                            "exception_type": type(exc).__name__,
                        },
                    )
                stop.wait(self.settings.worker_poll_seconds)
        finally:
            self.kafka.flush(12)


def main():
    settings = Settings()
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    Relay(
        Store(
            settings.database_url, topic=settings.kafka_topic, dlq_topic=settings.kafka_dlq_topic
        ),
        settings,
    ).run(stop)


if __name__ == "__main__":
    main()
