from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # App
    app_name: str = "Signal Craft Inhouse Tools API"
    debug: bool = False
    cors_origins: list[str] = ["http://localhost:5173"]

    # MongoDB
    mongodb_uri: str
    mongodb_db: str = "signal_craft"

    # PostgreSQL
    postgres_uri: str

    # Google Cloud Storage (legacy: wav_files 라우트용)
    gcs_bucket_name: str = ""
    gcs_signed_url_expiration: int = 3600  # seconds

    # Slices: raw bucket (cross-project, read-only) / sliced bucket (current project)
    raw_bucket_name: str = ""
    sliced_bucket_name: str = ""

    # Cloud Scheduler OIDC — service account email; empty = skip verification (dev)
    scheduler_service_account: str = ""

    class Config:
        env_file = ".env"


settings = Settings()
