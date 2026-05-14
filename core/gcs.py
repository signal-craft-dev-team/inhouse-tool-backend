from datetime import timedelta
from google.cloud import storage
from .config import settings

_client: storage.Client | None = None


def get_gcs_client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client()
    return _client


def generate_signed_url(blob_name: str) -> str:
    client = get_gcs_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    blob = bucket.blob(blob_name)
    return blob.generate_signed_url(
        expiration=timedelta(seconds=settings.gcs_signed_url_expiration),
        method="GET",
        version="v4",
    )
