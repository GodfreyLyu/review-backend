# Configuration, migration and recovery

Start from `.env.example`. `QUEUE_CAPACITY` counts all queued/running work; `MAX_ACTIVE_PER_USER` and `SUBMISSION_RATE_LIMIT` are shared through PostgreSQL across API replicas. `TASK_DEADLINE_SECONDS` includes queue time. `INFERENCE_TIMEOUT_SECONDS` is a fixed whole-review execution budget starting at the first claim. Socket timeout is only a bound on one network call; it never proves remote termination. Keep inference terminal-result retention longer than the longest expected backend outage/reconciliation window.

`WORKER_LEASE_SECONDS` must comfortably exceed the maximum socket+DB round-trip and scheduler stalls. Heartbeat runs between short RPCs. `RELAY_LEASE_SECONDS` should exceed the 12-second Kafka confirmation wait; default 30 seconds. Tune Kafka authentication/TLS using `KAFKA_SECURITY_PROTOCOL`, SASL and certificate settings. PostgreSQL TLS is configured in `DATABASE_URL` (`sslmode=verify-full` plus trusted CA). Use secure cookies and an actual HTTPS Origin outside loopback development. Generic Thrift is framed binary TCP; protect cross-host traffic with an authenticated mTLS sidecar/tunnel and network policy, keeping worker's endpoint on loopback. Do not use a round-robin service spanning independent inference instances.

The application-only Kubernetes template requires a namespace, externally provisioned PostgreSQL/Kafka, real secret stores, trusted certificates, a worker mTLS sidecar and an immutable image reference. It is not installed by this migration. Set graceful termination longer than application grace + RPC cancellation/reconciliation. The worker poll drain budget is not a hard process-exit deadline: executor cleanup can extend it by bounded RPC recovery and database I/O. The template allows 90 seconds; SIGKILL still falls back to fenced lease recovery. API, relay and worker scale/deploy independently; the persisted single slot still limits actual execution to one review. Additional workers improve failure replacement, not model throughput.

## Database changes

Back up PostgreSQL and validate backup restore before a production migration. Run explicit migrations once before starting a new release:

```bash
.venv/bin/review-migrate
```

SQL migration files are ordered, versioned and checksum-checked under `app/persistence/migrations`; migration uses a PostgreSQL advisory lock. API startup never creates schemas. Missing migrations make readiness fail. Never edit a migration already applied to a real database; add a new version. Roll back application images only when their schema expectations remain compatible; data rollback requires a reviewed restore/migration procedure. No automatic downgrade or original-data import is provided.

## Failures and recovery

- **Broker down:** review/outbox transaction remains authoritative. Relay retries confirmed publication; API queries continue. Backlog eventually rejects additional work. Do not mark rows sent manually.
- **Database down:** API readiness fails; worker stops making progress and does not commit unfinished positions. Recover PostgreSQL; lease scheduler and saved remote IDs restore ownership safely.
- **Relay dies after send:** owner lease expires, event republishes with the same ID. Do not delete the original event to hide duplication.
- **Worker dies after result:** completed result remains in PostgreSQL. Kafka replays and commits without regenerating.
- **Worker/rebalance dies during generation:** old fence cannot save results. A new owner reconciles the persisted instance/request before continuing. No new start is sent for that request.
- **Unknown remote status:** inspect the review's `execution_state`, `rpc_instance_id`, `rpc_request_id`, `rpc_section`, `recovery_count`, `lease_until` and slot owner using read-only SQL. Resolve routing/connectivity first. `recovery_required` automatically reconciles; `recovery_blocked` retains the slot after `MAX_RECOVERY_ATTEMPTS` (default 8) and needs operator intervention.

Resume a blocked reconciliation only after repairing its endpoint. The command schedules the saved attempt and identity; it does not clear the slot or issue start:

```bash
.venv/bin/review-recover REVIEW_UUID
```

If inference restarted and no longer knows the old request, `instance_changed`/`not_found` alone cannot prove native work exited. Confirm the old service process and its supervised native process have actually terminated using runtime/platform evidence. This repository deliberately has no unsafe force-unlock endpoint. A manual data repair after such evidence requires a separately reviewed operational change; it must not clear identity merely because a deadline expired.

DLQ entries contain references/reasons rather than source/results. Inspect PostgreSQL `kafka_dlq` and the configured DLQ topic. Correct unsupported messages at their producing version; never bulk replay poison payloads. Retry exhaustion of safely stopped work produces an execution DLQ reference. Recovery exhaustion is distinct from a terminal review and keeps capacity occupied until reconciled.

`MAX_RETRIES`, `RETRY_BASE_SECONDS`, `RETRY_MAX_SECONDS`, `MAX_RECOVERY_ATTEMPTS` and `START_WATCHDOG_SECONDS` are persisted-state scheduling policies. A relay must stay deployed because it owns recovery/watchdog scheduling as well as publication. Unsent/outbox history and review history retention need an explicit operational retention policy; do not purge records still referenced by retries/consumers. This first version favors auditable retained events over automatic data deletion.

## Release and credentials

Run independent unit/functional/build checks, publish immutable backend wheel/image from this repository and deploy the three commands separately. Service image and contract version are independent. Update contracts only after the separately released package passes this backend's checks. Rotate `SIGNING_SECRET` with the expectation that existing sessions are invalidated. Rotate the read-only contracts deploy key in GitHub without changing source code. No credential or model file belongs in Git.

## Test boundaries and process fault barriers

`REVIEW_TEST_PAUSE_AFTER_PUBLISH_FILE` and `REVIEW_TEST_PAUSE_AFTER_RESULT_FILE` pause only under `ENVIRONMENT=test`, creating a marker at the exact confirmed-send/before-mark and committed-result/before-offset boundaries. Functional tests SIGKILL the process there, then prove recovery. These are process test hooks with no HTTP routes and no effect in local/production mode.

Set `BACKEND_TEST_COMPOSE_FAILURES=1` only for this repository's isolated local Compose resources to exercise actual Kafka and PostgreSQL stop/start faults. The test restores each service in `finally`; it never calls Minikube, Argo CD or source-project commands.

```bash
BACKEND_TEST_COMPOSE_FAILURES=1 .venv/bin/python scripts/check.py --functional
```
