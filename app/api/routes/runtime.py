"""API status does not establish inference health; workers reconcile it separately."""

from fastapi import APIRouter, Request

from app.api.dependencies import AppSettings, ReviewStore
from app.health import check_readiness

router = APIRouter(prefix="/api/v1")


@router.get("/runtime")
def runtime(request: Request, settings: AppSettings, store: ReviewStore) -> dict:
    observation = check_readiness(store)
    if observation.error_code:
        request.state.error_code = observation.error_code
    return {
        "deployment_environment": settings.deployment_environment,
        "service_status": observation.status,
        "accepting_submissions": observation.status == "ready",
        "model_id": settings.expected_model_id,
        "model_revision": settings.expected_model_revision,
        "device": None,
        "quantization": None,
        "inference_status": "observed_by_worker",
    }
