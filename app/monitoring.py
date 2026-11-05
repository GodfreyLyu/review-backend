"""Content-free API observations; worker and inference health are separate."""

import time
from functools import wraps

from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily

from app.logging import SAFE_ERROR_CODES, SAFE_ROUTES
from app.persistence.storage import Store

HTTP_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})
HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)


def best_effort(method):
    """Observability failure must not change admission behavior."""

    @wraps(method)
    def record(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception:
            return None

    return record


class QueueCollector:
    def __init__(self, store: Store, capacity: int) -> None:
        self.store, self.capacity = store, capacity

    def collect(self):
        yield GaugeMetricFamily(
            "review_queue_capacity", "Configured outstanding review capacity.", value=self.capacity
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
        yield GaugeMetricFamily("review_queue_depth", "Persisted waiting reviews.", value=depth)
        yield GaugeMetricFamily(
            "review_queue_oldest_age_seconds",
            "Age of the oldest waiting review.",
            value=max(0, time.time() - oldest) if oldest is not None else 0,
        )
        yield GaugeMetricFamily(
            "review_metrics_collection_success",
            "Whether the queue snapshot was collected.",
            value=1,
        )


class Metrics:
    def __init__(self, store: Store, capacity: int) -> None:
        self.registry = CollectorRegistry()
        self.registry.register(QueueCollector(store, capacity))
        self.http_requests = Counter(
            "review_http_requests_total",
            "API responses excluding metrics scrapes.",
            ("method", "route", "status"),
            registry=self.registry,
        )
        self.http_duration = Histogram(
            "review_http_request_duration_seconds",
            "Time until response headers start.",
            ("method", "route"),
            buckets=HTTP_BUCKETS,
            registry=self.registry,
        )
        self.submissions = Counter(
            "review_submissions_total",
            "New reviews admitted by this API process.",
            registry=self.registry,
        )
        self.rejections = Counter(
            "review_rejections_total",
            "Admission rejections by this API process.",
            ("reason",),
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

    def render(self) -> bytes:
        return generate_latest(self.registry)
