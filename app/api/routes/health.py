"""HTTP process liveness and database/schema readiness."""

from fastapi import APIRouter, Request, Response

from app.api.dependencies import ReviewStore
from app.health import check_readiness

router = APIRouter(prefix="/health")


@router.get("/live")
def live() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/ready")
def ready(request: Request, response: Response, store: ReviewStore) -> dict[str, str]:
    observation = check_readiness(store)
    if observation.error_code:
        response.status_code = 503
        request.state.error_code = observation.error_code
    return {"status": observation.status}
