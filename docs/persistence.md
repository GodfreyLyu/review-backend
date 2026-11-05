# PostgreSQL transactions and recovery

PostgreSQL is the authoritative store for accounts, hashed sessions, review input,
validated results, execution identity, leases, retry schedules, outbox events and DLQ
references. No migration accesses the original application's SQLite or DynamoDB data.
Run the explicit migration command before starting a new version:

```bash
.venv/bin/review-migrate
```

Migration SQL is packaged in the backend wheel. `schema_migrations` records each
version and SHA-256; an applied migration with different bytes, or an unknown newer
version, is rejected. Apply changes as new numbered files. Application processes do
not create or upgrade schemas at startup. API readiness checks the database and
installed versions; inference availability does not affect history queries.

## Admission

`Store.create_review` locks the singleton `admission_lock` row. Within that same
transaction it checks the user/request-key mapping, compares the original source
and language, checks total outstanding capacity and per-user active limits, updates
the shared submission quota, and inserts the review and initial outbox event. The
API returns HTTP 202 only after commit. A repeated key with identical input returns
the saved task before quota or capacity checks; different input returns HTTP 409.

`QUEUE_CAPACITY` counts all outstanding queued and running reviews, including
retries and unresolved recovery. This bounded backlog is deliberately stricter than
the former in-process waiting queue. Login quotas and submission quotas are stored
in PostgreSQL fixed one-minute buckets. Buckets are shared by API replicas; they are
not the previous process-local sliding window. Authenticated sessions remain signed,
HTTP-only, SameSite=Strict cookies, with origin and CSRF checks on mutations.

## Outbox and scheduling

Outbox rows carry review references, event IDs, schema/execution versions, attempt,
trace ID and timestamps. They do not repeat source or generated content. The relay
claims due rows with `FOR UPDATE SKIP LOCKED`, using a finite lease. It waits for
Kafka delivery confirmation before marking a row sent. A crash after publish but
before the mark causes the same event ID to be published again after lease expiry.

Retries and slot-contention delays are persisted as `outbox.available_at`; Kafka
messages are only published once that time is due. A new worker does not depend on
a predecessor's timers. The relay also republishes overdue unstarted reviews,
expires queued task deadlines, and schedules expired-lease reconciliation. There is
no Debezium dependency.

## Worker ownership and uncertain inference

The worker claims a review with a database transaction, increments its fencing token
and obtains the durable `default` inference slot. Only an unexpired matching fence
may heartbeat, record an RPC intent, persist a section, finish, or schedule a retry.
The first claim persists `started_at` and the whole-review execution deadline;
recovery and section boundaries do not extend that execution budget. The separate
task deadline also bounds waiting and retry time.

Before sending `startGeneration`, the worker records the exact inference instance,
request ID, section and expected model identity in PostgreSQL. Successful sections
are saved individually. A timeout, transport failure, worker crash or expired lease
does not prove that the GPU stopped. Recovery queries/cancels that exact persisted
instance/request first. Unknown or missing remote state retains the RPC intent and
inference slot. No new start is sent merely because a lease or socket timeout expired.

`recovery_required` is a persisted execution state while the public review status
remains `running`. Automatic recovery has a bounded count and capped exponential
backoff. Expired active leases also spend recovery budget; overdue redispatch while
no worker is available does not. At exhaustion, `recovery_blocked` keeps the slot and
remote identity and emits a DLQ reference. The operator recovery command resumes
reconciliation of the same identity; it never authorizes an unverified new start.
See the recovery instructions in the project README.

If review A retains the inference slot while its recovery message is delayed, a
queued review B is atomically handed back to a delayed outbox event without spending
an execution attempt. B's original Kafka offset can then be acknowledged and the
worker can poll A's recovery message, even in the same partition. This avoids a
partition waiting behind a resource that only a later recovery record can release.

Normal retries are bounded by `MAX_RETRIES` plus the initial execution, and the
original task deadline. Poison Kafka messages are quarantined by source topic,
partition and offset with a payload digest, not raw untrusted contents. Exhausted
execution/recovery emits backend-owned DLQ reference messages. DLQ publication is
itself through the same durable outbox.

## Delivery boundary and tests

The system uses at-least-once delivery plus database idempotence and fencing. It
does not provide end-to-end exactly-once execution. A Kafka offset may be committed
only after a terminal result or a durable retry/recovery/quarantine handoff. The
consumer pauses while its one bounded execution runs, continues polling separately,
and never commits a later position over an unfinished message in the same partition.

The PostgreSQL functional tests use an explicitly named test database and exercise
concurrent admission, key replay/conflict, session sharing, quotas, outbox lease
recovery, stale fences, retained uncertain slots, delayed retries, deadlines and
recovery exhaustion. Tests simulate expired timestamps in that dedicated database;
real Kafka/Thrift process and crash tests are separate backend functional tests.
Production failover, backup restoration, broker replication and GPU behavior require
additional environment-specific validation. The supplied single-node local setup
does not establish production high availability.
