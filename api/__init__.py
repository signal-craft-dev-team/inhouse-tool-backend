from fastapi import APIRouter
from .routes import wav_files, sensors

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(wav_files.router)
api_router.include_router(sensors.router)  # prefix: /servers
