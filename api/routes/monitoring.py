from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from motor.motor_asyncio import AsyncIOMotorDatabase
from core.database import get_pg_session, get_mongo_db

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/places")
async def list_places(
    search: str = Query(""),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    sort: str = Query("asc", pattern="^(asc|desc)$"),
    session: AsyncSession = Depends(get_pg_session),
):
    direction = "ASC" if sort == "asc" else "DESC"
    skip = (page - 1) * page_size
    like = f"%{search}%"

    total = (await session.execute(text("""
        SELECT COUNT(*)
        FROM customer c
        JOIN place p ON p.customer_id = c.id
        WHERE c.customer_name ILIKE :like OR p.place_name ILIKE :like
    """), {"like": like})).scalar()

    rows = (await session.execute(text(f"""
        SELECT
            c.id         AS customer_id,
            c.customer_name,
            p.id         AS place_id,
            p.place_name,
            p.place_address,
            COUNT(es.id) AS server_count
        FROM customer c
        JOIN place p ON p.customer_id = c.id
        LEFT JOIN edgeserver es ON es.place_id = p.id
        WHERE c.customer_name ILIKE :like OR p.place_name ILIKE :like
        GROUP BY c.id, c.customer_name, p.id, p.place_name, p.place_address
        ORDER BY c.customer_name {direction}, p.place_name {direction}
        LIMIT :limit OFFSET :skip
    """), {"like": like, "limit": page_size, "skip": skip})).mappings().all()

    return {"total": total, "page": page, "page_size": page_size, "items": [dict(r) for r in rows]}


def _parse_range(date_from: str, date_to: str) -> tuple[datetime, datetime]:
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
    return start, end


@router.get("/server-status")
async def get_server_status(
    server_id: str,
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    start, end = _parse_range(date_from, date_to)

    docs = await db["server_status_logs"].find(
        {"server_id": server_id, "timestamp": {"$gte": start, "$lt": end}},
        {"_id": 0, "timestamp": 1, "changes": 1},
    ).sort("timestamp", 1).to_list(length=10000)

    return {
        "server_id": server_id,
        "events": [
            {
                "timestamp": d["timestamp"].isoformat(),
                "status": d.get("changes", {}).get("server_status"),
            }
            for d in docs
        ],
    }


@router.get("/sensor-status")
async def get_sensor_status(
    server_id: str,
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    start, end = _parse_range(date_from, date_to)

    docs = await db["sensor_status_logs"].find(
        {"server_id": server_id, "timestamp": {"$gte": start, "$lt": end}},
        {"_id": 0, "timestamp": 1, "device_name": 1, "sensor_type": 1},
    ).sort("timestamp", 1).to_list(length=10000)

    return {
        "server_id": server_id,
        "events": [
            {
                "timestamp": d["timestamp"].isoformat(),
                "device_name": d.get("device_name"),
                "sensor_type": d.get("sensor_type"),
            }
            for d in docs
        ],
    }


@router.get("/errors")
async def get_errors(
    server_id: str,
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    db: AsyncIOMotorDatabase = Depends(get_mongo_db),
):
    start, end = _parse_range(date_from, date_to)
    query = {"server_id": server_id, "received_at": {"$gte": start, "$lt": end}}
    skip = (page - 1) * page_size

    total = await db["error_logs"].count_documents(query)
    docs = await db["error_logs"].find(
        query, {"_id": 0}
    ).sort("received_at", -1).skip(skip).limit(page_size).to_list(length=page_size)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [
            {
                "received_at": d["received_at"].isoformat() if d.get("received_at") else None,
                "level": d.get("level"),
                "event": d.get("event"),
                "detail": d.get("detail"),
            }
            for d in docs
        ],
    }
