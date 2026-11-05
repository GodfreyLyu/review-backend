"""HTTP adapters for review admission and user-scoped history."""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query

from app.api.auth import Session
from app.api.dependencies import Reviews, ReviewStore
from app.api.schemas import (
    API_ERROR_RESPONSES,
    HistoryPage,
    ReviewAccepted,
    ReviewDetail,
    ReviewInput,
)

router = APIRouter(prefix="/api/v1/reviews", responses=API_ERROR_RESPONSES)


@router.post("", status_code=202, response_model=ReviewAccepted)
def submit(payload: ReviewInput, session: Session, service: Reviews) -> ReviewAccepted:
    review = service.submit(
        session["user_id"],
        payload.source_code,
        payload.language,
        str(payload.client_request_id) if payload.client_request_id else None,
    )
    return ReviewAccepted.model_validate(review)


@router.get("", response_model=HistoryPage)
def history(
    session: Session,
    store: ReviewStore,
    limit: Annotated[int, Query(ge=1, le=50)] = 20,
    before: UUID | None = None,
) -> HistoryPage:
    page = store.history(session["user_id"], limit, str(before) if before else None)
    return HistoryPage.model_validate(page)


@router.get("/{review_id}", response_model=ReviewDetail)
def detail(review_id: UUID, session: Session, store: ReviewStore) -> ReviewDetail:
    return ReviewDetail.model_validate(store.get_review(session["user_id"], str(review_id)))
