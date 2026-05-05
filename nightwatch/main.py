from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from nightwatch.api import incidents_router, metrics_router, runtime_config_router
from nightwatch.config import get_settings


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="NightWatch incident ingestion and auto-remediation service.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.get_cors_allow_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(incidents_router)
    app.include_router(metrics_router)
    app.include_router(runtime_config_router)
    return app


app = create_app()