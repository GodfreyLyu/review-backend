"""Per-application, content-free Prometheus observations with bounded labels."""

import time
from functools import wraps

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily

from app.inference.generation import GenerationMetrics
from app.inference.prompts import SECTION_SPECS
from app.logging import SAFE_ERROR_CODES, SAFE_ROUTES
from app.persistence.storage import Store

HTTP_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})
COORDINATOR_STATES = (
    "initializing",
    "inference_connecting",
    "ready",
    "inference_draining",
    "inference_stuck",
    "startup_or_storage_failure",
)
HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
REVIEW_BUCKETS = (0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1200, 3600)


def best_effort(method):
    """An observation failure must never change admission or inference behavior."""

    @wraps(method)
    def record(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception:
            return None

    return record


class QueueCollector:
    """Read one SQLite snapshot without creating storage or running writable probes."""

    def __init__(self, store: Store, capacity: int) -> None:
        self.store, self.capacity = store, capacity

    def collect(self):
        yield GaugeMetricFamily(
            "review_queue_capacity", "Configured waiting queue capacity.", value=self.capacity
        )
        try:
            depth, oldest = self.store.queue_snapshot()
        except Exception:
            yield GaugeMetricFamily(
                "review_metrics_collection_success",
                "Whether the queue snapshot was collected.",
                value=0,
            )
            return
        yield GaugeMetricFamily(
            "review_queue_depth", "Reviews waiting in persisted storage.", value=depth
        )
        yield GaugeMetricFamily(
            "review_queue_oldest_age_seconds",
            "Age since original submission of the oldest queued review; zero for an empty queue.",
            value=max(0, time.time() - oldest) if oldest is not None else 0,
        )
        yield GaugeMetricFamily(
            "review_metrics_collection_success",
            "Whether the queue snapshot was collected.",
            value=1,
        )


class CoordinatorCollector:
    """Take one state snapshot so a concurrent transition cannot break the one-hot series."""

    def __init__(self) -> None:
        self.state = "initializing"

    def collect(self):
        state = self.state
        observation = GaugeMetricFamily(
            "review_coordinator_state",
            "One-hot last operational coordinator state.",
            labels=("state",),
        )
        for value in COORDINATOR_STATES:
            observation.add_metric([value], int(value == state))
        yield observation


class Metrics:
    def __init__(self, store: Store, capacity: int) -> None:
        self.registry = CollectorRegistry()
        self.registry.register(QueueCollector(store, capacity))
        self.coordinator = CoordinatorCollector()
        self.registry.register(self.coordinator)
        self.http_requests = Counter(
            "review_http_requests_total",
            "HTTP responses started, excluding metrics scrapes.",
            ("method", "route", "status"),
            registry=self.registry,
        )
        self.http_duration = Histogram(
            "review_http_request_duration_seconds",
            "Time until HTTP response headers start.",
            ("method", "route"),
            buckets=HTTP_BUCKETS,
            registry=self.registry,
        )
        self.submissions = Counter(
            "review_submissions_total",
            "New reviews admitted by this process.",
            registry=self.registry,
        )
        self.rejections = Counter(
            "review_rejections_total",
            "Review admission attempts rejected by service policy.",
            ("reason",),
            registry=self.registry,
        )
        self.completions = Counter(
            "review_completions_total",
            "Review attempts persisted as completed or failed.",
            ("outcome",),
            registry=self.registry,
        )
        # These process-local event counts are known to start at zero. Expose a
        # baseline before the first event so low-traffic rate queries include it.
        # Reported token counters stay absent until terminal metadata is observed.
        for code in sorted(SAFE_ERROR_CODES):
            self.rejections.labels(code)
            self.completions.labels(code)
        self.completions.labels("completed")
        self.worker_busy = Gauge(
            "review_worker_busy",
            "Whether a review executor call is still running, including drain.",
            registry=self.registry,
        )
        self.queue_wait = Histogram(
            "review_queue_wait_seconds",
            "Original submission to attempt start, including recovery.",
            buckets=REVIEW_BUCKETS,
            registry=self.registry,
        )
        self.duration = Histogram(
            "review_duration_seconds",
            "Original submission to persisted terminal review state.",
            buckets=REVIEW_BUCKETS,
            registry=self.registry,
        )
        self.execution_duration = Histogram(
            "review_execution_duration_seconds",
            "Review executor duration, including cancellation drain.",
            buckets=REVIEW_BUCKETS,
            registry=self.registry,
        )
        self.generation_duration = Histogram(
            "review_generation_duration_seconds",
            "Observed section call duration including first content.",
            ("section",),
            buckets=REVIEW_BUCKETS,
            registry=self.registry,
        )
        self.first_content_duration = Histogram(
            "review_first_content_duration_seconds",
            "Section call to its first nonempty content.",
            ("section",),
            buckets=REVIEW_BUCKETS,
            registry=self.registry,
        )
        # A zero observation count is a baseline, not a measured zero duration.
        # Initialize the fixed sections so the first review contributes to rates.
        for section, _, _ in SECTION_SPECS:
            self.generation_duration.labels(section)
            self.first_content_duration.labels(section)
        self.input_tokens = Counter(
            "review_input_tokens_total",
            "Input tokens reported for completed section responses.",
            ("section",),
            registry=self.registry,
        )
        self.generated_tokens = Counter(
            "review_generated_tokens_total",
            "Generated tokens reported for completed section responses.",
            ("section",),
            registry=self.registry,
        )
        self.output_limit = Counter(
            "review_output_limit_total",
            "Completed section responses that reached the output limit.",
            ("section",),
            registry=self.registry,
        )

    @best_effort
    def http_response(self, method: str, route: str | None, status: int, duration: float) -> None:
        method = method if method in HTTP_METHODS else "OTHER"
        route = route if route in SAFE_ROUTES else "unknown"
        status = str(status) if 100 <= status <= 599 else "unknown"
        self.http_requests.labels(method, route, status).inc()
        self.http_duration.labels(method, route).observe(max(0, duration))

    @best_effort
    def submitted(self) -> None:
        self.submissions.inc()

    @best_effort
    def rejected(self, reason: str) -> None:
        self.rejections.labels(reason if reason in SAFE_ERROR_CODES else "internal_error").inc()

    @best_effort
    def finished(self, outcome: str, duration: float) -> None:
        outcome = outcome if outcome in SAFE_ERROR_CODES | {"completed"} else "inference_failed"
        self.completions.labels(outcome).inc()
        self.duration.observe(max(0, duration))

    @best_effort
    def started(self, queue_wait: float) -> None:
        self.queue_wait.observe(max(0, queue_wait))

    @best_effort
    def worker_started(self) -> None:
        self.worker_busy.set(1)

    @best_effort
    def worker_finished(self, duration: float) -> None:
        self.worker_busy.set(0)
        self.execution_duration.observe(max(0, duration))

    @best_effort
    def coordinator_state(self, state: str) -> None:
        state = state if state in COORDINATOR_STATES else "startup_or_storage_failure"
        self.coordinator.state = state

    @best_effort
    def generation(self, metrics: GenerationMetrics) -> None:
        for section, _, _ in SECTION_SPECS:
            observation = metrics.sections.get(section)
            if observation is None:
                continue
            if observation.generation_ms is not None:
                self.generation_duration.labels(section).observe(observation.generation_ms / 1000)
            if observation.first_token_ms is not None:
                self.first_content_duration.labels(section).observe(
                    observation.first_token_ms / 1000
                )
            self.input_tokens.labels(section).inc(observation.input_tokens)
            self.generated_tokens.labels(section).inc(observation.generated_tokens)
            self.output_limit.labels(section).inc(int(observation.limit_reached))

    def render(self) -> bytes:
        return generate_latest(self.registry)
