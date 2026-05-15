from datetime import datetime, timedelta, timezone
from typing import Annotated

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token
from motor.motor_asyncio import AsyncIOMotorDatabase
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.database import get_mongo_db, get_pg_session
from core.gcs import generate_sliced_signed_url, get_raw_bucket, get_sliced_bucket
from services.audio_slicer import run_slicing_batch

router = APIRouter(prefix="/slices", tags=["slices"])

KST = timezone(timedelta(hours=9))


def _serialize(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    if "source_id" in doc:
        doc["source_id"] = str(doc["source_id"])
    return doc


def _parse_object_id(raw: str) -> ObjectId:
    try:
        return ObjectId(raw)
    except (InvalidId, TypeError):
        raise HTTPException(status_code=422, detail="Invalid id format")


def _verify_scheduler_token(authorization: str | None) -> None:
    if not settings.scheduler_service_account:
        return  # dev: skip verification
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing OIDC token")
    token = authorization[7:]
    try:
        request = google_requests.Request()
        info = id_token.verify_oauth2_token(token, request)
        if info.get("email") != settings.scheduler_service_account:
            raise HTTPException(status_code=403, detail="Unauthorized service account")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid OIDC token")


# GET /slices/ — 슬라이스 목록
@router.get("/")
async def list_slices(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    server_id: str | None = Query(None),
    sensor_device_id: str | None = Query(None, description="MAC suffix (e.g. A0F262EC9088)"),
    date_from: datetime | None = Query(None, description="ISO 8601 UTC"),
    date_to: datetime | None = Query(None, description="ISO 8601 UTC"),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    query: dict = {}
    if server_id:
        query["server_id"] = server_id
    if sensor_device_id:
        query["sensor_device_id"] = sensor_device_id
    if date_from or date_to:
        query["timestamp"] = {}
        if date_from:
            query["timestamp"]["$gte"] = date_from
        if date_to:
            query["timestamp"]["$lte"] = date_to

    skip = (page - 1) * page_size
    col = db["audio_slices"]
    total = await col.count_documents(query)
    docs = await col.find(query).sort("timestamp", -1).skip(skip).limit(page_size).to_list(length=page_size)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_serialize(d) for d in docs],
    }


# GET /slices/daily-status — 날짜별 처리 현황 (캘린더용)
@router.get("/daily-status")
async def daily_status(
    server_id: str | None = Query(None),
    month: str | None = Query(None, description="YYYYMM, default: current month"),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    if month:
        try:
            month_start_kst = datetime.strptime(month, "%Y%m").replace(tzinfo=KST)
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid month format. Expected YYYYMM.")
    else:
        now_kst = datetime.now(KST)
        month_start_kst = now_kst.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Next month start in KST → convert to UTC for query
    if month_start_kst.month == 12:
        next_month_kst = month_start_kst.replace(year=month_start_kst.year + 1, month=1)
    else:
        next_month_kst = month_start_kst.replace(month=month_start_kst.month + 1)

    utc_start = month_start_kst.astimezone(timezone.utc)
    utc_end = next_month_kst.astimezone(timezone.utc)

    match: dict = {"timestamp": {"$gte": utc_start, "$lt": utc_end}}
    if server_id:
        match["server_id"] = server_id

    pipeline = [
        {"$match": match},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp", "timezone": "+09:00"}},
            "total": {"$sum": 1},
            "done": {"$sum": {"$cond": [{"$eq": ["$slice_status", "done"]}, 1, 0]}},
            "processing": {"$sum": {"$cond": [{"$eq": ["$slice_status", "processing"]}, 1, 0]}},
            "pending": {"$sum": {"$cond": [{"$in": ["$slice_status", ["pending", None]]}, 1, 0]}},
            "failed": {"$sum": {"$cond": [{"$eq": ["$slice_status", "failed"]}, 1, 0]}},
        }},
        {"$sort": {"_id": 1}},
    ]

    docs = await db["audio_upload_logs"].aggregate(pipeline).to_list(length=None)

    def _day_status(d: dict) -> str:
        if d["done"] == d["total"]:
            return "done"
        if d["processing"] > 0:
            return "processing"
        if d["pending"] == d["total"]:
            return "pending"
        return "partial"

    return {
        "month": month_start_kst.strftime("%Y%m"),
        "items": [
            {
                "date": d["_id"],
                "status": _day_status(d),
                "total": d["total"],
                "done": d["done"],
                "processing": d["processing"],
                "pending": d["pending"],
                "failed": d["failed"],
            }
            for d in docs
        ],
    }


# GET /slices/{id}/download-url — presigned URL 발급
@router.get("/{slice_id}/download-url")
async def get_download_url(
    slice_id: str,
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    oid = _parse_object_id(slice_id)
    doc = await db["audio_slices"].find_one({"_id": oid}, {"gcs_path": 1, "status": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Slice not found")
    if doc.get("status") != "success":
        raise HTTPException(status_code=409, detail=f"Slice is not ready (status: {doc.get('status')})")

    gcs_path = doc.get("gcs_path")
    if not gcs_path:
        raise HTTPException(status_code=500, detail="GCS path not set")

    url = generate_sliced_signed_url(gcs_path)
    return {"slice_id": slice_id, "url": url, "expires_in": settings.gcs_signed_url_expiration}


# POST /slices/trigger — Cloud Scheduler 호출용
@router.post("/trigger")
async def trigger_slicing(
    background_tasks: BackgroundTasks,
    date: str | None = Query(None, description="처리 대상 날짜 YYYYMMDD (KST), 기본값: 어제"),
    server_id: str | None = Query(None),
    authorization: Annotated[str | None, Header()] = None,
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
    session: AsyncSession = Depends(get_pg_session),
):
    _verify_scheduler_token(authorization)

    raw_bucket = get_raw_bucket()
    sliced_bucket = get_sliced_bucket()

    background_tasks.add_task(
        run_slicing_batch,
        db, session, raw_bucket, sliced_bucket, date, server_id,
    )
    return {
        "status": "triggered",
        "date": date or (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d"),
        "server_id": server_id,
    }
