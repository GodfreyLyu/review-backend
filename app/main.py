"""Application composition and lifecycle. Run with app.main:create_app --factory."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.http_errors import register_error_handlers
from app.api.middleware import RequestGuard
from app.api.routes import auth, health, metrics, reviews, runtime
from app.config import Settings
from app.coordinator import Coordinator
from app.inference.client import ReviewClient
from app.inference.rpc.client import ThriftReviewClient
from app.logging import configure_logging
from app.monitoring import Metrics
from app.persistence.storage import Store
from app.persistence.users import DynamoUsers, UserStore
from app.rate_limit import RateLimiter
from app.review_service import ReviewService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.coordinator.start()
    try:
        yield
    finally:
        await app.state.coordinator.close()


def create_app(
    settings: Settings | None = None,
    *,
    review_client: ReviewClient | None = None,
    users: UserStore | None = None,
) -> FastAPI:
    config = settings or Settings()
    configure_logging(config)
    app = FastAPI(
        title="Local Code Review API",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs" if config.enable_api_docs else None,
        redoc_url=None,
        openapi_url="/openapi.json" if config.enable_api_docs else None,
    )
    app.state.settings = config
    app.state.store = store = Store(config.data_dir)
    app.state.metrics = monitoring = Metrics(store, config.queue_capacity)
    app.state.users = user_store = users or DynamoUsers(config)
    if review_client is None:
        review_client = ThriftReviewClient(config, metrics=monitoring)
    app.state.review_client = review_client
    app.state.coordinator = Coordinator(store, user_store, review_client, config, monitoring)
    app.state.limiter = limiter = RateLimiter()
    app.state.review_service = ReviewService(
        config, store, app.state.coordinator, limiter, monitoring
    )
    app.add_middleware(RequestGuard, metrics=monitoring)
    register_error_handlers(app)
    for router in (auth.router, reviews.router, health.router, runtime.router, metrics.router):
        app.include_router(router)
    return app
