import asyncio
from fastapi import APIRouter, HTTPException, Query
from core.config import settings
from core.gcs import get_gcs_client, _get_sliced_gcs_client

router = APIRouter(prefix="/storage", tags=["storage"])


def _get_client_for_bucket(bucket_name: str):
    """버킷 이름에 따라 적절한 GCS 클라이언트 반환."""
    if bucket_name == settings.raw_bucket_name:
        return get_gcs_client()
    return _get_sliced_gcs_client()


def _scan_bucket(bucket_name: str, prefix: str | None) -> dict:
    """동기: 버킷 내 파일 수 및 용량 집계."""
    client = _get_client_for_bucket(bucket_name)
    try:
        blobs = client.list_blobs(bucket_name, prefix=prefix)
        total_files = 0
        total_bytes = 0
        for blob in blobs:
            total_files += 1
            total_bytes += blob.size or 0
    except Exception as e:
        raise RuntimeError(str(e))

    return {
        "bucket": bucket_name,
        "prefix": prefix,
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / 1024 / 1024, 2),
        "total_gb": round(total_bytes / 1024 / 1024 / 1024, 3),
    }


@router.get("/stats")
async def get_storage_stats(
    bucket: str = Query(..., description="GCS 버킷 이름"),
    prefix: str | None = Query(None, description="경로 prefix (예: sliced/4487c40fff8e40abb04ef2fdd53e1499)"),
):
    try:
        result = await asyncio.to_thread(_scan_bucket, bucket, prefix)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result
