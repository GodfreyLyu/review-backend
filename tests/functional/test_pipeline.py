"""Run this repository's real API/relay/worker against PostgreSQL + Kafka.

Only the remote LLM implementation is substituted, via the published Thrift package.
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from tests.functional.thrift_stub import running_stub

pytestmark = pytest.mark.functional
ROOT = Path(__file__).resolve().parents[2]
SOURCE = "def average(values):\n    return sum(values) / len(values)"


def wait_for(check, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError("Condition did not become true before timeout")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Processes:
    def __init__(self, env, directory):
        self.env, self.directory, self.running = env, directory, []

    def start(self, name, **overrides):
        log = open(self.directory / f"{name}-{uuid4().hex}.log", "wb")
        module = {"api": "app.entrypoints", "relay": "app.relay.main", "worker": "app.worker.main"}[
            name
        ]
        function = "api" if name == "api" else "main"
        process = subprocess.Popen(
            [sys.executable, "-c", f"from {module} import {function}; {function}()"],
            cwd=ROOT,
            env=dict(self.env, **overrides),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.running.append((process, log))
        return process

    def close(self):
        for process, _ in self.running:
            if process.poll() is None:
                process.terminate()
        for process, log in self.running:
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            log.close()
        failures = [p for p, _ in self.running if p.returncode not in (0, -15, -9)]
        if failures:
            for path in self.directory.glob("*.log"):
                print(path.name, path.read_text()[-5000:])


@pytest.fixture
def stack(database, store, tmp_path):
    brokers = os.environ.get("BACKEND_TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not brokers:
        pytest.skip("Set BACKEND_TEST_KAFKA_BOOTSTRAP_SERVERS for real Kafka")
    topic = "review-functional-" + uuid4().hex
    admin = AdminClient({"bootstrap.servers": brokers})
    for future in admin.create_topics(
        [
            NewTopic(topic, num_partitions=2, replication_factor=1),
            NewTopic(topic + "-dlq", num_partitions=1, replication_factor=1),
        ]
    ).values():
        future.result(timeout=20)
    with running_stub() as (handler, thrift_port):
        port = free_port()
        env = dict(
            os.environ,
            ENVIRONMENT="test",
            SIGNING_SECRET="functional-signing-secret-" + "x" * 32,
            DATABASE_URL=database,
            COOKIE_SECURE="false",
            ALLOWED_ORIGIN="http://localhost:5173",
            THRIFT_HOST="127.0.0.1",
            THRIFT_PORT=str(thrift_port),
            API_PORT=str(port),
            KAFKA_BOOTSTRAP_SERVERS=brokers,
            KAFKA_TOPIC=topic,
            KAFKA_DLQ_TOPIC=topic + "-dlq",
            KAFKA_GROUP_ID="group-" + uuid4().hex,
            WORKER_LEASE_SECONDS="2",
            RELAY_LEASE_SECONDS="20",
            RETRY_BASE_SECONDS="0.2",
            START_WATCHDOG_SECONDS="2",
            THRIFT_RECOVERY_SECONDS="0.5",
            TASK_DEADLINE_SECONDS="60",
            LOGIN_RATE_LIMIT="100",
            SUBMISSION_RATE_LIMIT="60",
        )
        # API writes this topic into outbox; direct test Store must use it too.
        store.topic, store.dlq_topic = topic, topic + "-dlq"
        processes = Processes(env, tmp_path)
        processes.start("api")
        url = f"http://127.0.0.1:{port}"

        def ready():
            try:
                return httpx.get(url + "/health/ready", timeout=1).status_code == 200
            except httpx.TransportError:
                return False

        try:
            wait_for(ready)
            with httpx.Client(
                base_url=url, headers={"origin": env["ALLOWED_ORIGIN"]}, timeout=3
            ) as client:
                response = client.post(
                    "/api/v1/auth/register",
                    json={
                        "login_id": "user" + uuid4().hex[:10],
                        "password": "functional-password-1234",
                    },
                )
                assert response.status_code == 201, response.text
                client.headers["x-csrf-token"] = response.json()["csrf_token"]
                yield client, store, handler, processes, topic, brokers
        finally:
            processes.close()
            for future in admin.delete_topics([topic, topic + "-dlq"]).values():
                future.result(timeout=20)


def submit(client):
    payload = {"source_code": SOURCE, "language": "python", "client_request_id": str(uuid4())}
    response = client.post("/api/v1/reviews", json=payload)
    assert response.status_code == 202, response.text
    return response.json()["review_id"], payload


def detail(client, review_id):
    response = client.get("/api/v1/reviews/" + review_id)
    assert response.status_code == 200, response.text
    return response.json()


def completed(client, review_id):
    row = detail(client, review_id)
    if row["status"] == "failed":
        raise AssertionError(row)
    return row if row["status"] == "completed" else None


def test_real_api_outbox_kafka_worker_persistence(stack):
    client, store, handler, processes, topic, brokers = stack
    review_id, payload = submit(client)
    # The API has committed before it returns and no inference process was started.
    assert not handler.starts
    with store.connection() as db:
        row = db.execute("SELECT * FROM outbox WHERE review_id=%s", (review_id,)).fetchone()
        assert row is not None
    replay = client.post("/api/v1/reviews", json=payload)
    assert replay.status_code == 202 and replay.json()["review_id"] == review_id
    conflict = client.post(
        "/api/v1/reviews", json=dict(payload, source_code=SOURCE + "\n# changed")
    )
    assert conflict.status_code == 409
    processes.start("relay")
    processes.start("worker")
    result = wait_for(lambda: completed(client, review_id))
    assert all(
        "## " + section in result["review_result"]
        for section in ("Summary", "Findings", "Suggestions")
    )
    assert len(handler.starts) == 3
    # History/readiness remain usable with no running inference dependency.
    handler.unreachable = True
    assert client.get("/health/ready").status_code == 200
    assert client.get("/api/v1/reviews").json()["items"][0]["review_id"] == review_id


def test_relay_confirmation_crash_republishes_duplicate_without_regeneration(stack, tmp_path):
    client, store, handler, processes, topic, brokers = stack
    review_id, _ = submit(client)
    marker = tmp_path / "published"
    relay = processes.start("relay", REVIEW_TEST_PAUSE_AFTER_PUBLISH_FILE=str(marker))
    wait_for(marker.exists)
    relay.kill()
    relay.wait(timeout=3)
    processes.start("worker")
    wait_for(lambda: completed(client, review_id))
    with store.connection() as db:
        db.execute("UPDATE outbox SET claim_until=0 WHERE review_id=%s", (review_id,))
    processes.start("relay")
    wait_for(lambda: sent(store, review_id))
    time.sleep(1)
    assert len(handler.starts) == 3
    messages = read_topic(brokers, topic, 2)
    assert len(messages) == 2
    assert messages[0]["event_id"] == messages[1]["event_id"]


def sent(store, review_id):
    with store.connection() as db:
        return (
            db.execute("SELECT sent_at FROM outbox WHERE review_id=%s", (review_id,)).fetchone()[
                "sent_at"
            ]
            is not None
        )


def read_topic(brokers, topic, count):
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": "inspect-" + uuid4().hex,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    deadline = time.monotonic() + 15
    try:
        while len(messages) < count and time.monotonic() < deadline:
            message = consumer.poll(0.2)
            if message is not None and not message.error():
                messages.append(json.loads(message.value()))
    finally:
        consumer.close()
    return messages


def test_worker_result_crash_before_commit_replays_without_regeneration(stack, tmp_path):
    client, store, handler, processes, topic, brokers = stack
    review_id, _ = submit(client)
    marker = tmp_path / "finished"
    processes.start("relay")
    worker = processes.start("worker", REVIEW_TEST_PAUSE_AFTER_RESULT_FILE=str(marker))
    wait_for(marker.exists)
    assert completed(client, review_id)
    worker.kill()
    worker.wait(timeout=3)
    processes.start("worker")

    def committed():
        consumer = Consumer(
            {
                "bootstrap.servers": brokers,
                "group.id": processes.env["KAFKA_GROUP_ID"],
                "enable.auto.commit": False,
            }
        )
        try:
            return any(
                p.offset > 0
                for p in consumer.committed(
                    [TopicPartition(topic, 0), TopicPartition(topic, 1)], timeout=3
                )
            )
        finally:
            consumer.close()

    wait_for(committed)
    assert len(handler.starts) == 3


def test_worker_killed_remote_unknown_keeps_slot_and_reconciles_same_request(stack):
    client, store, handler, processes, topic, brokers = stack
    handler.delay = 20
    review_id, _ = submit(client)
    processes.start("relay")
    worker = processes.start("worker")
    wait_for(lambda: len(handler.starts) == 1)
    original = handler.starts[0].request_id
    worker.kill()
    worker.wait(timeout=3)
    handler.unreachable = True
    processes.start("worker")
    wait_for(lambda: detail(client, review_id).get("execution_state") == "recovery_required")
    time.sleep(2.5)
    assert [r.request_id for r in handler.starts] == [original]
    # Restored lookup/cancel must reconcile original ID before another section is started.
    handler.unreachable = False
    handler.delay = 0
    with handler.lock:
        handler.requests[original][1] = time.monotonic()
    wait_for(
        lambda: (
            bool(handler.cancels) or detail(client, review_id)["status"] in {"completed", "failed"}
        ),
        timeout=35,
    )
    assert all(instance == handler.instance_id for instance, _ in handler.cancels)
    assert len([r for r in handler.starts if r.request_id == original]) == 1


def test_real_group_rebalance_cancels_or_recovers_original_request(stack):
    client, store, handler, processes, topic, brokers = stack
    handler.delay = 20
    review_id, _ = submit(client)
    processes.start("relay")
    processes.start("worker")
    wait_for(lambda: len(handler.starts) == 1)
    original = handler.starts[0].request_id
    processes.start("worker")
    wait_for(lambda: bool(handler.cancels), timeout=30)
    handler.delay = 0
    with handler.lock:
        for entry in handler.requests.values():
            entry[1] = time.monotonic()
    wait_for(lambda: completed(client, review_id))
    assert len([r for r in handler.starts if r.request_id == original]) == 1
    assert len(handler.starts) <= 4
    assert handler.cancels[0] == (handler.instance_id, original)


def test_graceful_shutdown_reconciles_remote_before_exit(stack):
    client, store, handler, processes, topic, brokers = stack
    handler.delay = 20
    review_id, _ = submit(client)
    processes.start("relay")
    worker = processes.start("worker")
    wait_for(lambda: len(handler.starts) == 1)
    original = handler.starts[0].request_id
    worker.terminate()
    assert worker.wait(timeout=12) == 0
    assert (handler.instance_id, original) in handler.cancels
    assert detail(client, review_id)["execution_state"] == "retry_wait"
    assert len(handler.starts) == 1
    handler.delay = 0
    processes.start("worker")
    wait_for(lambda: completed(client, review_id))
    assert len([r for r in handler.starts if r.request_id == original]) == 1


def test_queued_other_user_cannot_block_uncertain_slot_recovery(stack):
    client, store, handler, processes, topic, brokers = stack
    handler.delay = 20
    first, _ = submit(client)
    processes.start("relay")
    worker = processes.start("worker")
    wait_for(lambda: len(handler.starts) == 1)
    original = handler.starts[0].request_id
    worker.kill()
    worker.wait(timeout=3)
    handler.unreachable = True
    processes.start("worker")
    wait_for(lambda: detail(client, first)["execution_state"] == "recovery_required")
    with httpx.Client(
        base_url=client.base_url, headers={"origin": processes.env["ALLOWED_ORIGIN"]}
    ) as other:
        registration = other.post(
            "/api/v1/auth/register",
            json={"login_id": "user" + uuid4().hex[:10], "password": "functional-password-1234"},
        )
        assert registration.status_code == 201
        other.headers["x-csrf-token"] = registration.json()["csrf_token"]
        second, _ = submit(other)
        time.sleep(1)
        assert [request.request_id for request in handler.starts] == [original]
        handler.unreachable = False
        handler.delay = 0
        with handler.lock:
            handler.requests[original][1] = time.monotonic()
        wait_for(lambda: completed(client, first))
        wait_for(lambda: completed(other, second))
    assert len(handler.starts) == 6
    assert len([request for request in handler.starts if request.request_id == original]) == 1


def test_poison_message_is_durably_quarantined_and_dlq_published(stack):
    from confluent_kafka import Producer

    client, store, handler, processes, topic, brokers = stack
    review_id, _ = submit(client)
    producer = Producer({"bootstrap.servers": brokers})
    producer.produce(topic, key=b"invalid-key", value=b"not-json")
    for target, attempt in ((str(uuid4()), 1), (review_id, 999)):
        envelope = {
            "event_id": str(uuid4()),
            "review_id": target,
            "attempt": attempt,
            "trace_id": str(uuid4()),
            "created_at": time.time(),
            "schema_version": 1,
            "execution_version": "review-v1",
        }
        producer.produce(topic, key=target.encode(), value=json.dumps(envelope).encode())
    assert producer.flush(10) == 0
    processes.start("relay")
    processes.start("worker")

    def quarantined():
        with store.connection() as db:
            return db.execute("SELECT count(*) AS n FROM kafka_dlq").fetchone()["n"] == 3

    wait_for(quarantined)
    messages = read_topic(brokers, topic + "-dlq", 3)
    assert len(messages) == 3
    assert all(message["source_topic"] == topic for message in messages)
    assert "not-json" not in json.dumps(messages)
    wait_for(lambda: completed(client, review_id))
    assert len(handler.starts) == 3


def test_real_kafka_and_postgres_temporary_outages_recover(stack):
    if os.environ.get("BACKEND_TEST_COMPOSE_FAILURES") != "1":
        pytest.skip("Set BACKEND_TEST_COMPOSE_FAILURES=1 for isolated Compose stop/start faults")
    client, store, handler, processes, topic, brokers = stack
    review_id, _ = submit(client)
    subprocess.run(
        ["docker", "compose", "stop", "kafka"], cwd=ROOT, check=True, capture_output=True
    )
    try:
        processes.start("relay")
        processes.start("worker")
        time.sleep(1)
        assert client.get("/health/ready").status_code == 200
        assert not handler.starts
        assert not sent(store, review_id)
    finally:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--wait", "kafka"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        )
    handler.delay = 20
    wait_for(lambda: len(handler.starts) == 1)
    original = handler.starts[0].request_id
    subprocess.run(
        ["docker", "compose", "stop", "postgres"], cwd=ROOT, check=True, capture_output=True
    )
    try:
        assert client.get("/health/ready").status_code == 503
        time.sleep(2.5)
        assert len(handler.starts) == 1
    finally:
        subprocess.run(
            ["docker", "compose", "up", "-d", "--wait", "postgres"],
            cwd=ROOT,
            check=True,
            capture_output=True,
        )
    handler.delay = 0
    with handler.lock:
        handler.requests[original][1] = time.monotonic()
    wait_for(lambda: completed(client, review_id), timeout=40)
    assert len(handler.starts) == 3
    assert len([request for request in handler.starts if request.request_id == original]) == 1
