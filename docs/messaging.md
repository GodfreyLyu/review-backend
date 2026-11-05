# Review messaging and execution ownership

`app/messaging/contracts/review.jobs.v1.schema.json` and `decode_job` define the
backend-owned review message. The Kafka key is the canonical `review_id` UUID.
`event_id` identifies one outbox publication, `schema_version=1` identifies its
envelope, `execution_version=review-v1` selects the section/prompt workflow,
`attempt` identifies the execution attempt, and `trace_id` preserves submission
correlation. Source and generated text remain in PostgreSQL. Unknown versions,
invalid keys, extra fields, oversized payloads, and malformed envelopes are
quarantined before acknowledging their offsets. Quarantine persists only a digest
and source topic/partition/offset, then emits a metadata-only DLQ outbox event.
DLQ records without a review reference use their event ID as the Kafka key.
Syntactically valid records that reference an unknown review or an impossible
future attempt are also quarantined; they cannot hold a paused consumer forever.

The API transaction holds the database admission lock through idempotency, quota,
capacity, review insert, and outbox insert. The polling relay claims one due row
at a time with a renewable-by-expiry ownership lease, waits for Kafka delivery
confirmation, then conditionally marks that row sent. A crash after publish and
before marking produces a duplicate with the same event ID. Kafka producer
idempotence reduces duplicates within a producer session; it does not establish
end-to-end exactly-once execution.

Each worker has one inference executor. The consumer thread pauses assigned
partitions while that executor runs and continues polling, including during long
generation and cancellation. Both automatic commits and automatic offset storage
are disabled. The worker commits precisely the active message's offset plus one,
synchronously, only after a result, terminal failure, or durable retry/recovery
handoff. It cannot commit a later record over an unfinished one. A failed commit
is retried without generating again. Rebalance callbacks signal cancellation and
invalidate the old assignment generation; they neither block on inference nor
commit that assignment's unfinished offsets. PostgreSQL lease/fence checks prevent
an old executor from saving a section or result after ownership changes.

When a different review owns the inference slot, claiming a queued job persists
a delayed redispatch with the same execution attempt. Its delivery can then be
acknowledged, allowing the consumer to reach the slot owner's recovery messages
without blocking every partition behind that waiting job.

The single database inference slot is shared by all worker replicas and matches
the initial one-instance, one-in-flight-review operating model. Before every
start, the worker commits instance ID, request ID, section, and expected model
identity. It never reissues a persisted start on recovery. It queries or cancels
that exact instance/request until a terminal snapshot is confirmed. Confirmed
sections are saved individually and reused. RPC timeouts, expired leases,
`not_found`, changed instances, and failed cancellation do not prove GPU exit.
Unknown execution keeps its slot and durable identity in a recovery state.
Bounded automatic recovery eventually requires an operator rather than silently
starting another generation. Normal retry attempts use exponential backoff capped
by `RETRY_MAX_SECONDS`; scheduling lives in PostgreSQL `available_at` and due
outbox rows, not sleeping Kafka consumers or in-memory timers.

The relay also performs lease expiry and overdue-queued-task checks. A lease
expiry increments the fence before publishing recovery responsibility. Task and
persisted execution deadlines bound admission/queueing and total model execution
respectively. A deadline while RPC state is unknown retains recovery ownership;
it does not release the inference slot.

TLS/SASL Kafka settings and PostgreSQL TLS connection parameters are configuration
inputs. The development Compose broker is single-node and plaintext; successful
local checks do not establish production durability, authentication, or HA.

## Test crash barriers

When and only when `ENVIRONMENT=test`,
`REVIEW_TEST_PAUSE_AFTER_PUBLISH_FILE` and `REVIEW_TEST_PAUSE_AFTER_RESULT_FILE`
name filesystem barriers. The relay touches the former after Kafka confirmation
and before marking sent. The executor touches the latter after result commit and
before returning to the consumer's offset commit. The process waits until the
file is removed or shutdown is requested. Backend functional tests kill the
process at these boundaries to verify real replay behavior. These are not HTTP
routes and are disabled in local/production environments.

Kafka client behavior follows the [Confluent Python client documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html).
