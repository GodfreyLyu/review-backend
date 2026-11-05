"""Bounded Kafka poll loop. Long RPC work never runs on the consumer thread."""

import logging
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from confluent_kafka import KafkaException, TopicPartition

from app.application.execution import InvalidDelivery, ReviewExecutor
from app.config import Settings
from app.messaging.events import InvalidEvent, decode_job
from app.messaging.kafka import consumer
from app.persistence.storage import Store

logger = logging.getLogger("review.worker")


class Worker:
    def __init__(self, store, settings, kafka=None, executor=None):
        self.store, self.settings = store, settings
        self.kafka = kafka if kafka is not None else consumer(settings)
        self.execute = executor if executor is not None else ReviewExecutor(store, settings)
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="review-execution")
        self.active = None
        self.generation = 0

    def on_assign(self, consumer, partitions):
        self.generation += 1
        consumer.assign(partitions)
        if self.active is not None:
            consumer.pause(partitions)

    def on_revoke(self, _consumer, _partitions):
        self.generation += 1
        if self.active is not None:
            self.active["stop"].set()
        # Never wait for inference or commit unfinished offsets inside rebalance callbacks.

    def _pause(self):
        assignment = self.kafka.assignment()
        if assignment:
            self.kafka.pause(assignment)

    def _resume(self):
        assignment = self.kafka.assignment()
        if assignment:
            self.kafka.resume(assignment)

    def _process(self, message, stop):
        try:
            event = decode_job(message.value(), message.key())
            return self.execute.handle(event, stop)
        except (InvalidEvent, InvalidDelivery) as error:
            self.store.quarantine_message(
                message.topic(),
                message.partition(),
                message.offset(),
                str(error),
                message.value(),
            )
            return True

    def _submit(self, message):
        self._pause()
        stop = threading.Event()
        self.active = {
            "message": message,
            "stop": stop,
            "generation": self.generation,
            "future": self.pool.submit(self._process, message, stop),
            "retry_at": 0,
        }

    def _finish(self):
        active = self.active
        if active is None:
            return
        future = active["future"]
        if not future.done():
            return
        if active["generation"] != self.generation:
            # Retrieve exceptions from the abandoned future before discarding its result.
            if not future.cancelled():
                future.exception()
            # Ownership changed: DB recovery or terminal state is durable, Kafka will replay.
            self.active = None
            self._resume()
            return
        try:
            durable = future.result()
        except Exception as exc:
            logger.warning(
                "worker_processing_retry",
                extra={
                    "exception_type": type(exc).__name__,
                },
            )
            durable = False
        if durable:
            try:
                # Exactly this message's offset+1; no later partition position can pass it.
                result = self.kafka.commit(message=active["message"], asynchronous=False)
                if result and any(part.error is not None for part in result):
                    raise RuntimeError("offset_commit_unconfirmed")
            except (KafkaException, RuntimeError) as exc:
                logger.warning(
                    "worker_commit_retry",
                    extra={
                        "exception_type": type(exc).__name__,
                    },
                )
                return
            self.active = None
            self._resume()
            return
        if time.monotonic() >= active["retry_at"]:
            # Keep the partition paused until claim succeeds or a durable handoff is observed.
            active["retry_at"] = time.monotonic() + self.settings.retry_base_seconds
            active["future"] = self.pool.submit(self._process, active["message"], active["stop"])

    def tick(self):
        self._finish()
        message = self.kafka.poll(self.settings.worker_poll_seconds)
        if message is None:
            return
        if message.error():
            logger.warning("worker_kafka_poll_failure")
            return
        if self.active is None:
            self._submit(message)
        else:
            # Defensive protection for prefetched delivery around pause/reassignment.
            # Rewind this unprocessed record; it must never be covered by a later commit.
            self.kafka.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
            self._pause()

    def run(self, stop):
        self.kafka.subscribe(
            [self.settings.kafka_topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_revoke,
        )
        try:
            while not stop.is_set():
                try:
                    self.tick()
                    Path(
                        os.environ.get("PROCESS_HEARTBEAT_FILE", "/tmp/review-worker.heartbeat")
                    ).touch()
                except Exception as exc:
                    logger.warning(
                        "worker_dependency_unavailable",
                        extra={
                            "exception_type": type(exc).__name__,
                        },
                    )
                    stop.wait(self.settings.worker_poll_seconds)
            if self.active is not None:
                self.active["stop"].set()
                self._pause()
                deadline = time.monotonic() + self.settings.shutdown_grace_seconds
                while self.active is not None and time.monotonic() < deadline:
                    self._finish()
                    self.kafka.poll(self.settings.worker_poll_seconds)
        finally:
            if self.active is not None:
                self.active["stop"].set()
            self.kafka.close()
            self.pool.shutdown(wait=True, cancel_futures=True)


def main():
    settings = Settings()
    logging.basicConfig(level=logging.INFO)
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())
    Worker(
        Store(
            settings.database_url, topic=settings.kafka_topic, dlq_topic=settings.kafka_dlq_topic
        ),
        settings,
    ).run(stop)


if __name__ == "__main__":
    main()
