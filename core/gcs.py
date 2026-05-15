from datetime import timedelta
from google.auth import compute_engine
from google.auth.transport.requests import Request
from google.cloud import storage
from .config import settings

# Project A SA key (GOOGLE_APPLICATION_CREDENTIALS) — raw 버킷 읽기용
_raw_client: storage.Client | None = None

# GCE VM 기본 SA (메타데이터 서버) — sliced 버킷 쓰기용 (Project B)
_sliced_client: storage.Client | None = None


def get_gcs_client() -> storage.Client:
    """Legacy: wav_files 라우트용 (Project A SA)"""
    global _raw_client
    if _raw_client is None:
        _raw_client = storage.Client()
    return _raw_client


def _get_sliced_gcs_client() -> storage.Client:
    """GCE 메타데이터 서버 인증 — Project B 버킷 접근용"""
    global _sliced_client
    if _sliced_client is None:
        try:
            credentials = compute_engine.Credentials()
            credentials.refresh(Request())
            _sliced_client = storage.Client(credentials=credentials)
        except Exception:
            # 로컬 환경 등 GCE 메타데이터 서버가 없으면 기본 클라이언트 사용
            _sliced_client = storage.Client()
    return _sliced_client


def get_raw_bucket() -> storage.Bucket:
    if not settings.raw_bucket_name:
        raise RuntimeError("RAW_BUCKET_NAME is not set in environment")
    return get_gcs_client().bucket(settings.raw_bucket_name)


def get_sliced_bucket() -> storage.Bucket:
    if not settings.sliced_bucket_name:
        raise RuntimeError("SLICED_BUCKET_NAME is not set in environment")
    return _get_sliced_gcs_client().bucket(settings.sliced_bucket_name)


def generate_signed_url(blob_name: str) -> str:
    client = get_gcs_client()
    bucket = client.bucket(settings.gcs_bucket_name)
    blob = bucket.blob(blob_name)
    return blob.generate_signed_url(
        expiration=timedelta(seconds=settings.gcs_signed_url_expiration),
        method="GET",
        version="v4",
    )


def generate_sliced_signed_url(blob_name: str) -> str:
    blob = get_sliced_bucket().blob(blob_name)
    return blob.generate_signed_url(
        expiration=timedelta(seconds=settings.gcs_signed_url_expiration),
        method="GET",
        version="v4",
    )
