"""Typed HTTP adapters to per-application dependencies assembled by create_app."""

from typing import Annotated

from fastapi import Depends, Request

from app.config import Settings
from app.coordinator import Coordinator
from app.inference.client import ReviewClient
from app.monitoring import Metrics
from app.persistence.storage import Store
from app.persistence.users import UserStore
from app.rate_limit import RateLimiter
from app.review_service import ReviewService


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_store(request: Request) -> Store:
    return request.app.state.store


def get_users(request: Request) -> UserStore:
    return request.app.state.users


def get_review_client(request: Request) -> ReviewClient:
    return request.app.state.review_client


def get_limiter(request: Request) -> RateLimiter:
    return request.app.state.limiter


def get_coordinator(request: Request) -> Coordinator:
    return request.app.state.coordinator


def get_review_service(request: Request) -> ReviewService:
    return request.app.state.review_service


def get_metrics(request: Request) -> Metrics:
    return request.app.state.metrics


AppSettings = Annotated[Settings, Depends(get_settings)]
ReviewStore = Annotated[Store, Depends(get_store)]
Accounts = Annotated[UserStore, Depends(get_users)]
RequestLimiter = Annotated[RateLimiter, Depends(get_limiter)]
ReviewCoordinator = Annotated[Coordinator, Depends(get_coordinator)]
Reviews = Annotated[ReviewService, Depends(get_review_service)]

InferenceClient = Annotated[ReviewClient, Depends(get_review_client)]
Monitoring = Annotated[Metrics, Depends(get_metrics)]
