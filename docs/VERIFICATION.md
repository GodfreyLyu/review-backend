# Migration verification

Source commit: `a76de47f75362ef6f34fe21771383c70f90c5379`. Source worktree was clean and remained read-only. No original user data, credentials, model weights or deployed resources were migrated or modified.

## Executed locally (2026-11-05, macOS ARM64)

- Python 3.12 project `.venv`, hash-locked runtime/development installation and normal wheel installation from the **private published contract** succeeded. No editable/neighbor inference paths were used.
- Current unit/non-functional suite: **64 passed**. This covers admission policy, output validation, message handling, retry classification, fencing responses, RPC reconciliation and consumer/relay behavior without PostgreSQL/Kafka.
- Real PostgreSQL persistence suite: **14 passed**. It covers cross-connection capacity/user limits, transactional idempotency/outbox, session sharing/logout/CSRF/isolation, quota sharing, lease expiration/fencing, persistent deadline, recovery cap/operator resume and invalid future/unknown deliveries.
- Real API/Relay/Worker pipeline suite: **9 passed**, including actual Kafka and PostgreSQL stop/start failure injection. Poison coverage was then extended to valid unknown/future-attempt envelopes and rechecked with all persistence regressions (**15 passed** combined).
- Pipeline tests run this repository's actual three process entrypoints, real PostgreSQL 17.6 and Apache Kafka 4.1.1. Only the model endpoint is a backend-owned real TCP framed/binary Thrift double built from published contracts. No inference-service source/package, Ollama, GPU, frontend or cross-project E2E test is used.
- Crash barriers prove send-confirmed/before-outbox-mark and result-committed/before-offset windows by SIGKILL and restart. Other checks cover duplicate event IDs, rebalance, graceful cancellation/restart, expired leases/late fences, unknown execution, two-user slot recovery, poison DLQ and temporary infrastructure outages.
- Ruff lint/format, independent backend wheel and ARM64 Docker image build succeeded. The image was built from this repository plus the already hash-verified contract wheel.
- Private cross-repository Actions [contract access run 41948572032](https://github.com/GodfreyLyu/review-backend/actions/runs/41948572032) **passed**, including actual wheel retrieval, SHA256 verification and install using a newly provisioned read-only deploy key.

Independent clean HTTPS clone at commit `1c3e85a7d66dbb306e568a9b29260970f6efdda2` installed via the documented bootstrap and passed **64 unit tests** and **23 functional tests with actual Compose failures** (73.72 seconds), plus independent wheel build. Outside the checkout, backend and contract imports resolved to that clone's `.venv/site-packages`; all five CLI entrypoints loaded, packaged SQL/schema files were present, `uv pip check` reported 39 compatible packages, and no inference implementation was installed. The six shared IDEA run XML files parsed successfully and contained project-relative interpreter/work-directory paths. Older staging failures (concurrent test DB truncation, a 10ms test lease, and a rebalance-double timing assumption) were corrected; none are reported as successful checks. Tests are serialized against the dedicated database. Current fixtures force expiry only after durable intent; actual process crash/rebalance tests exercise the asynchronous boundary.

## Unverified boundaries

No real inference/model/GPU was used in backend tests. Kubernetes templates, mTLS, production authentication/TLS, backup/restore, production Kafka/PostgreSQL HA, actual GPU termination after a service restart and IDEA GUI configuration were not deployed or validated. Local Compose is a single-node development environment. New app database `review_backend` is distinct from destructive functional database `review_backend_test`.

The worker's configured drain grace is not an absolute process-exit limit: bounded RPC reconciliation may extend cleanup. Kubernetes allows a longer termination window; a forced kill relies on saved leases/remote intent. API metrics intentionally describe only API/DB observations, with worker/inference health separated.

Two upstream test-client deprecation warnings occur in the pinned Starlette/httpx combination; no test failure resulted. Tests never point at the original project or Minikube/Argo CD environment. Shared IDEA run configurations and separate project interpreters are delivered as files; opening/selecting the projects remains a manual GUI action.
