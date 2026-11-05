"""Internal Prometheus exposition, independent of sessions and readiness probes."""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST

from app.api.dependencies import Monitoring

router = APIRouter()


@router.get("/metrics", include_in_schema=False)
def metrics(monitoring: Monitoring) -> Response:
    return Response(monitoring.render(), headers={"Content-Type": CONTENT_TYPE_LATEST})
