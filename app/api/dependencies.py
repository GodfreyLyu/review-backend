"""HTTP dependencies for independently composed API instances."""

from typing import Annotated

from fastapi import Depends, Request

from app.application.admission import ReviewService
from app.config import Settings
from app.monitoring import Metrics
from app.persistence.storage import DatabaseRateLimiter, Store
from app.persistence.users import UserStore


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_store(request: Request) -> Store:
    return request.app.state.store


def get_users(request: Request) -> UserStore:
    return request.app.state.users


def get_limiter(request: Request) -> DatabaseRateLimiter:
    return request.app.state.limiter


def get_review_service(request: Request) -> ReviewService:
    return request.app.state.review_service


def get_metrics(request: Request) -> Metrics:
    return request.app.state.metrics


AppSettings = Annotated[Settings, Depends(get_settings)]
ReviewStore = Annotated[Store, Depends(get_store)]
Accounts = Annotated[UserStore, Depends(get_users)]
RequestLimiter = Annotated[DatabaseRateLimiter, Depends(get_limiter)]
Reviews = Annotated[ReviewService, Depends(get_review_service)]
Monitoring = Annotated[Metrics, Depends(get_metrics)]
