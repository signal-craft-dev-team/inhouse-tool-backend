from fastapi import APIRouter
from .routes import wav_files, sensors, dashboard, monitoring, slices

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(dashboard.router)
api_router.include_router(monitoring.router)
api_router.include_router(wav_files.router)
api_router.include_router(sensors.router)  # prefix: /servers
api_router.include_router(slices.router)   # prefix: /slices
