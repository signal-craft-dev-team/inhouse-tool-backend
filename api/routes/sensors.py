from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from core.database import get_pg_session

router = APIRouter(prefix="/servers", tags=["servers"])


# 2-4: 서버 목록 조회 — place, customer 정보 포함
@router.get("/")
async def list_servers(session: AsyncSession = Depends(get_pg_session)):
    result = await session.execute(text("""
        SELECT
            es.id,
            es.server_id,
            es.server_status,
            es.installation_machine,
            es.capture_duration_ms,
            es.upload_interval_ms,
            es.active_hours_start,
            es.active_hours_end,
            es.timezone,
            es.created_at,
            es.updated_at,
            p.id         AS place_id,
            p.place_name AS place_name,
            p.place_address    AS place_address,
            c.id         AS customer_id,
            c.customer_name
        FROM edgeserver es
        JOIN place p ON p.id = es.place_id
        JOIN customer c ON c.id = p.customer_id
        ORDER BY es.created_at DESC
    """))
    rows = result.mappings().all()
    return {"items": [dict(r) for r in rows]}


# 2-5: 특정 서버의 센서 목록 조회 (server_id = MongoDB의 server_id 필드와 동일한 hex 값)
@router.get("/{server_id}/sensors")
async def list_sensors(server_id: str, session: AsyncSession = Depends(get_pg_session)):
    # edgeserver.server_id(hex)로 먼저 서버 존재 확인
    srv = await session.execute(
        text("SELECT id FROM edgeserver WHERE server_id = :sid"),
        {"sid": server_id},
    )
    row = srv.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Server not found")

    result = await session.execute(
        text("""
            SELECT
                es.id,
                es.device_name,
                es.sensor_type,
                es.sensor_position,
                es.installation_machine,
                es.created_at,
                es.updated_at
            FROM edgesensor es
            WHERE es.edge_server_id = :server_uuid
            ORDER BY es.device_name
        """),
        {"server_uuid": row["id"]},
    )
    sensors = result.mappings().all()
    return {"server_id": server_id, "items": [dict(s) for s in sensors]}
