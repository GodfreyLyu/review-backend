"""HTTP-only composition. API startup never starts inference or a worker."""

from fastapi import FastAPI

from app.api.http_errors import register_error_handlers
from app.api.middleware import RequestGuard
from app.api.routes import auth, health, metrics, reviews, runtime
from app.application.admission import ReviewService
from app.config import Settings
from app.logging import configure_logging
from app.monitoring import Metrics
from app.persistence.storage import DatabaseRateLimiter, Store
from app.persistence.users import PostgresUsers, UserStore


def create_app(
    settings: Settings | None = None, *, store: Store | None = None, users: UserStore | None = None
) -> FastAPI:
    config = settings or Settings()
    configure_logging(config)
    app = FastAPI(
        title="Review Backend API",
        version="1.0.0",
        docs_url="/docs" if config.enable_api_docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if config.enable_api_docs else None,
    )
    app.state.settings = config
    app.state.store = store = store or Store(
        config.database_url, topic=config.kafka_topic, dlq_topic=config.kafka_dlq_topic
    )
    app.state.users = users or PostgresUsers(store)
    app.state.metrics = monitoring = Metrics(store, config.queue_capacity)
    app.state.limiter = DatabaseRateLimiter(store)
    app.state.review_service = ReviewService(
        store,
        max_chars=config.source_max_chars,
        capacity=config.queue_capacity,
        deadline_seconds=config.task_deadline_seconds,
        active_limit=config.max_active_per_user,
        submission_limit=config.submission_rate_limit,
        metrics=monitoring,
    )
    app.add_middleware(RequestGuard, metrics=monitoring)
    register_error_handlers(app)
    for router in (auth.router, reviews.router, health.router, runtime.router, metrics.router):
        app.include_router(router)
    return app
