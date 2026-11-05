"""Read-only, non-sensitive instance information. No cluster access is required."""

from fastapi import APIRouter, Request

from app.api.dependencies import AppSettings, InferenceClient, ReviewCoordinator, ReviewStore
from app.health import check_readiness
from app.inference.identity import model_identity

router = APIRouter(prefix="/api/v1")


@router.get("/runtime")
def runtime(
    request: Request,
    settings: AppSettings,
    coordinator: ReviewCoordinator,
    store: ReviewStore,
    review_client: InferenceClient,
) -> dict:
    observation = check_readiness(coordinator, store)
    # Runtime remains HTTP 200 while retaining the probe's diagnostic error code.
    if observation.error_code is not None:
        request.state.error_code = observation.error_code
    state = observation.status
    if coordinator.closing:
        state = "shutting_down"
    return {
        "deployment_environment": settings.deployment_environment,
        "service_status": state,
        "accepting_submissions": state == "ready",
        **model_identity(review_client),
    }
