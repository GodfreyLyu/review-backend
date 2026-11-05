# Review Backend

Independent HTTP API, PostgreSQL business persistence, polling outbox relay and Kafka review worker. No frontend, GPU, model runtime or other source checkout is required to build or test this repository.

The three application processes share this package but run and deploy independently:

- `review-api`: authentication, signed/HttpOnly sessions, CSRF, admission, history and queries.
- `outbox-relay`: PostgreSQL outbox publication, retry scheduling and abandoned-task recovery.
- `review-worker`: bounded review orchestration, validation and generic Thrift calls.

Python 3.12, uv, Git and authenticated GitHub CLI are required for local installation. Docker Compose supplies dedicated local PostgreSQL and real Kafka. Maven, Ollama and inference-service source are unnecessary here.

## Install and configure

Clone this private repository using your own GitHub access.

```bash
gh repo clone GodfreyLyu/review-backend
```

```bash
cd review-backend
```

```bash
python3.12 scripts/bootstrap.py --dev
```

Bootstrap installs hash-locked dependencies, fetches the **published** `local-llm-contract==1.0.0` wheel, verifies [contracts.lock.json](contracts.lock.json), then installs this project normally. No editable contract/source installation is used. A read-only deploy key is an alternative to GitHub CLI authentication:

```bash
python3.12 scripts/bootstrap.py --dev --ssh-key /path/to/read-only-key
```

```bash
cp .env.example .env
```

Replace the signing secret with a private random value. The sample database password and insecure cookie setting are exclusively for loopback local development. Never commit `.env`. PostgreSQL, sessions and reviews are new; this migration does not import original runtime data or user accounts.

## Start, migrate and stop

```bash
docker compose up -d --wait
```

```bash
.venv/bin/review-migrate
```

Run each process in its own terminal from this project root:

```bash
.venv/bin/review-api
```

```bash
.venv/bin/outbox-relay
```

```bash
.venv/bin/review-worker
```

The API listens on `127.0.0.1:8080`. Registration/login routes remain under `/api/v1/auth`; use the configured Origin and returned CSRF token for mutation requests. Reviews remain under `/api/v1/reviews`. `202` means the review and outbox event committed together, not that inference started. See [architecture](docs/ARCHITECTURE.md) for states and limits.

For container application processes, fetch the wheel first, copy `.env`, then use the optional app profile. Only the worker needs a separately running inference endpoint; configure its host/port without another source checkout. Docker Desktop's `host.docker.internal` is the default in this profile.

```bash
docker compose --profile app up -d --build
```

Stop terminal processes with Ctrl-C; they also handle SIGTERM. Stop containers while preserving test volumes:

```bash
docker compose --profile app down
```

## Tests and build

Unit checks need neither PostgreSQL nor Kafka:

```bash
.venv/bin/python scripts/check.py
```

Functional tests use a database whose name contains `test`, truncate only that explicitly configured database, create unique Kafka topics, run real API/relay/worker subprocesses and use an in-repository **real TCP Thrift server double** built solely from the published contracts. No real model, inference-service implementation or frontend is exercised.

```bash
docker compose up -d --wait
```

```bash
.venv/bin/python scripts/check.py --functional
```

Override `BACKEND_TEST_DATABASE_URL` and `BACKEND_TEST_KAFKA_BOOTSTRAP_SERVERS` for dedicated external test resources. Never point tests at application data. Failure-injection process barriers only activate with `ENVIRONMENT=test`. [Verification](docs/VERIFICATION.md) records actual results and boundaries.

```bash
.venv/bin/python -m build --wheel
```

```bash
docker build -t review-backend:local .
```

The Docker build takes only this repository and the verified `.artifacts` wheel fetched by bootstrap. CI independently reads the immutable private contract artifact using the repository secret `CONTRACTS_DEPLOY_KEY`; the key can read only inference-service. A missing/rotated key fails explicitly. The wheel is also available in the private [contracts release](https://github.com/GodfreyLyu/inference-service/releases/tag/contracts-v1.0.0). See [contract distribution](docs/CONTRACTS.md).

## IDEA

Open **this directory** as its own IDEA project and configure the Python plugin interpreter at `.venv/bin/python`. Do not open the parent directory as a combined project. Shared `.run` files provide Review API, Outbox Relay, Review Worker, Database Migrate, Unit Tests and Functional Tests; all use project-relative paths. Copy `.env`, start Compose, run Database Migrate, then launch the three run configurations independently. Python's debugger can attach separately to each process. No personal `.idea/workspace.xml` is committed. GUI project/interpreter selection remains a manual step; it was not performed by the migration agent.

## Operations

[Recovery and configuration](docs/OPERATIONS.md) covers TLS/authentication, leases, cancellation, delayed retries, DLQ and release/database migration. [deploy/backend.yaml](deploy/backend.yaml) is an application-only Kubernetes starting point, requiring your own namespace, secrets, TLS sidecar and immutable image digest. Local Compose uses single-node PostgreSQL and Kafka with replication factor 1 and loopback plaintext; successful local tests do not validate production HA. The original Minikube/Argo CD environment is untouched.

[Provenance](docs/provenance.md) records the read-only source commit. This repository owns review business rules and `review.jobs.v1`; inference-service owns the generic Thrift protocol and independently versioned contract package.

## Independent release

After committing/pushing a new package version and waiting for this repository's CI to pass for that exact commit, publish its private wheel and checksum:

```bash
.venv/bin/python scripts/publish.py
```

The script requires a clean checkout, checks wheel package/version/contents, checks exact-commit CI, atomically creates a new `backend-vVERSION` tag and creates a private Release. Existing tags/artifacts are never overwritten. `--dry-run` builds/validates locally without publication. The explicit `Release review backend` Actions workflow reruns this repository's complete checks and publishes the CI wheel; it uses the same contract-reader secret and needs repository content-write permission. If tag creation succeeds but upload fails, inspect and finish that same release manually without moving the tag or replacing an artifact. Publishing this wheel does not publish the contract, inference implementation or a complete platform image. The Dockerfile independently builds this project's runnable image for your registry/release process.
