"""Liveness and readiness probes."""

from fastapi import APIRouter, Request, Response

from app.api.dependencies import ReviewCoordinator, ReviewStore
from app.health import check_readiness
from app.logging import SAFE_ERROR_CODES

router = APIRouter(prefix="/health")


@router.get("/live")
def live(request: Request, response: Response, coordinator: ReviewCoordinator) -> dict[str, str]:
    response.status_code = 200 if coordinator.live else 503
    if not coordinator.live:
        request.state.error_code = (
            coordinator.state
            if coordinator.state in SAFE_ERROR_CODES
            else "startup_or_storage_failure"
        )
    return {"status": "alive" if coordinator.live else coordinator.state}


@router.get("/ready")
def ready(
    request: Request, response: Response, coordinator: ReviewCoordinator, store: ReviewStore
) -> dict[str, str]:
    observation = check_readiness(coordinator, store)
    if observation.error_code is not None:
        response.status_code = 503
        request.state.error_code = observation.error_code
    return {"status": observation.status}
