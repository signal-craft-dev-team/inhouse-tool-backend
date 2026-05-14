from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from core.database import get_mongo_db
from core.gcs import generate_signed_url

router = APIRouter(prefix="/wav-files", tags=["wav-files"])


def _serialize(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc


def _parse_object_id(raw: str) -> ObjectId:
    try:
        return ObjectId(raw)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=422, detail="Invalid file id format")


def _build_filter(
    date_from: datetime | None,
    date_to: datetime | None,
    server_id: str | None,
    sensor_id: str | None,
    status: str | None,
) -> dict:
    f: dict = {}
    if date_from or date_to:
        f["timestamp"] = {}
        if date_from:
            f["timestamp"]["$gte"] = date_from
        if date_to:
            f["timestamp"]["$lte"] = date_to
    if server_id:
        f["server_id"] = server_id
    if sensor_id:
        f[f"sensor_map.{sensor_id}"] = {"$exists": True}
    if status:
        f["status"] = status
    return f


# 2-1: WAV 파일 목록 조회 — 필터 + 페이지네이션
@router.get("/")
async def list_wav_files(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    date_from: datetime | None = Query(None, description="조회 시작일시 (ISO 8601)"),
    date_to: datetime | None = Query(None, description="조회 종료일시 (ISO 8601)"),
    server_id: str | None = Query(None),
    sensor_id: str | None = Query(None, description="sensor_map에 포함된 센서 ID"),
    status: str | None = Query(None, description="success | pending | failure"),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    query = _build_filter(date_from, date_to, server_id, sensor_id, status)
    skip = (page - 1) * page_size
    collection = db["audio_upload_logs"]

    total = await collection.count_documents(query)
    docs = await collection.find(query).skip(skip).limit(page_size).to_list(length=page_size)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_serialize(d) for d in docs],
    }


# 2-2: WAV 파일 단건 상세 조회
@router.get("/{file_id}")
async def get_wav_file(
    file_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    oid = _parse_object_id(file_id)
    doc = await db["audio_upload_logs"].find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="File not found")
    return _serialize(doc)


# 2-3: GCS Signed URL 발급 (다운로드 트리거)
@router.get("/{file_id}/download-url")
async def get_download_url(
    file_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    oid = _parse_object_id(file_id)
    doc = await db["audio_upload_logs"].find_one({"_id": oid}, {"gcs_path": 1, "status": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="File not found")

    if doc.get("status") != "success":
        raise HTTPException(status_code=409, detail=f"File is not ready (status: {doc.get('status')})")

    gcs_path = doc.get("gcs_path")
    if not gcs_path:
        raise HTTPException(status_code=500, detail="GCS path not set for this file")

    signed_url = generate_signed_url(gcs_path)
    return {"file_id": file_id, "url": signed_url, "expires_in": 3600}
