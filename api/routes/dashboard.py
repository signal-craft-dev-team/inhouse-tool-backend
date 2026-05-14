from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from core.database import get_pg_session

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get("/stats")
async def get_stats(session: AsyncSession = Depends(get_pg_session)):
    result = await session.execute(text("""
        SELECT
            (SELECT COUNT(*)                              FROM edgeserver)            AS total_servers,
            (SELECT COUNT(*) FROM edgeserver WHERE server_status = 'ONLINE')         AS online_servers,
            (SELECT COUNT(*) FROM edgeserver WHERE server_status = 'OFFLINE')        AS offline_servers,
            (SELECT COUNT(*)                              FROM edgesensor)            AS total_sensors,
            (SELECT COUNT(*)                              FROM customer)              AS total_customers,
            (SELECT COUNT(*)                              FROM place)                 AS total_places
    """))
    row = dict(result.mappings().first())
    return row
