from fastapi import APIRouter, Response

from nightwatch.metrics import render_metrics_payload


router = APIRouter(tags=["metrics"])


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    payload, content_type = render_metrics_payload()
    return Response(content=payload, media_type=content_type)