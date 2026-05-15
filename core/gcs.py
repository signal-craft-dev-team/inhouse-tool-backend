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
    """sliced 버킷 접근용 클라이언트.

    우선순위:
    1. SLICED_BUCKET_CREDENTIALS 키 파일 (지정 시)
    2. GCE IAM signBlob API (키 파일 없이 서명 가능, IAM Credentials API 필요)
    3. fallback: 기본 클라이언트 (signed URL 불가)
    """
    global _sliced_client
    if _sliced_client is None:
        if settings.sliced_bucket_credentials:
            from google.oauth2 import service_account
            creds = service_account.Credentials.from_service_account_file(
                settings.sliced_bucket_credentials,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            _sliced_client = storage.Client(credentials=creds)
        else:
            try:
                import google.auth
                from google.auth.iam import Signer
                from google.oauth2.service_account import Credentials as SACredentials

                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                auth_request = Request()
                creds.refresh(auth_request)

                sa_email = getattr(creds, "service_account_email", None)
                if sa_email:
                    signer = Signer(auth_request, creds, sa_email)
                    signing_creds = SACredentials(
                        signer=signer,
                        service_account_email=sa_email,
                        token_uri="https://oauth2.googleapis.com/token",
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    )
                    _sliced_client = storage.Client(credentials=signing_creds)
                else:
                    _sliced_client = storage.Client(credentials=creds)
            except Exception:
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
    blob = _get_sliced_gcs_client().bucket(settings.sliced_bucket_name).blob(blob_name)
    return blob.generate_signed_url(
        expiration=timedelta(seconds=settings.gcs_signed_url_expiration),
        method="GET",
        version="v4",
    )
