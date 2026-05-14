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

    # Google Cloud Storage
    gcs_bucket_name: str
    gcs_signed_url_expiration: int = 3600  # seconds

    class Config:
        env_file = ".env"


settings = Settings()
