import asyncio
import io
import logging
import wave
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

from bson import ObjectId
from google.cloud import storage
from motor.motor_asyncio import AsyncIOMotorDatabase
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

KST = timezone(timedelta(hours=9))


async def _build_sensor_lookup(server_id: str, session: AsyncSession) -> dict[str, tuple[str, str]]:
    """Returns {mac_suffix: (installation_machine, device_name)} for a given server."""
    result = await session.execute(
        text("""
            SELECT es.device_name, es.installation_machine
            FROM edgesensor es
            JOIN edgeserver esrv ON esrv.id = es.edge_server_id
            WHERE esrv.server_id = :server_id
        """),
        {"server_id": server_id},
    )
    lookup: dict[str, tuple[str, str]] = {}
    for row in result.mappings():
        device_name: str = row["device_name"]
        installation_machine: str = row["installation_machine"] or ""
        # sensor_map element is a MAC suffix contained in device_name
        # e.g. device_name="scts-0003-A0F262EC9088", mac_suffix="A0F262EC9088"
        lookup[device_name] = (installation_machine, device_name)
    return lookup


def _find_sensor(mac_suffix: str, lookup: dict[str, tuple[str, str]]) -> tuple[str, str] | None:
    """Returns (installation_machine, device_name) by substring match."""
    for device_name, info in lookup.items():
        if mac_suffix in device_name:
            return info
    return None


def _split_wav(
    raw_bytes: bytes,
    sensor_map: list[str],
    lookup: dict[str, tuple[str, str]],
    timestamp_utc: datetime,
    server_id: str,
) -> list[dict]:
    """Synchronous: splits raw WAV bytes into per-sensor segments. Returns list of slice metadata dicts."""
    kst = timestamp_utc.astimezone(KST)
    date_str = kst.strftime("%Y%m%d")
    time_str = kst.strftime("%H%M%S")

    slices = []
    with wave.open(io.BytesIO(raw_bytes)) as wav:
        nframes = wav.getnframes()
        framerate = wav.getframerate()
        sampwidth = wav.getsampwidth()
        nchannels = wav.getnchannels()
        segment_frames = nframes // len(sensor_map)

        for i, mac_suffix in enumerate(sensor_map):
            sensor_info = _find_sensor(mac_suffix, lookup)
            if sensor_info:
                installation_machine, device_name = sensor_info
                filename = f"{date_str}_{time_str}_{installation_machine}_{device_name}.wav"
            else:
                installation_machine = None
                device_name = f"unknown_{mac_suffix}"
                filename = f"{date_str}_{time_str}_{device_name}.wav"

            gcs_path = f"sliced/{server_id}/{date_str}/{filename}"

            wav.setpos(i * segment_frames)
            raw_frames = wav.readframes(segment_frames)

            buf = io.BytesIO()
            with wave.open(buf, "wb") as out:
                out.setnchannels(nchannels)
                out.setsampwidth(sampwidth)
                out.setframerate(framerate)
                out.writeframes(raw_frames)

            slices.append({
                "sensor_index": i,
                "sensor_device_id": mac_suffix,
                "installation_machine": installation_machine,
                "device_name": device_name,
                "gcs_path": gcs_path,
                "wav_bytes": buf.getvalue(),
                "duration_ms": int(segment_frames / framerate * 1000),
            })

    return slices


def _upload_slices(
    slices: list[dict],
    raw_bucket: storage.Bucket,
    sliced_bucket: storage.Bucket,
    source_gcs_path: str,
) -> None:
    """Synchronous: downloads raw WAV then uploads each slice. Raises on error."""
    for s in slices:
        blob = sliced_bucket.blob(s["gcs_path"])
        blob.upload_from_string(s["wav_bytes"], content_type="audio/wav")


async def slice_audio_upload(
    doc: dict,
    db: AsyncIOMotorDatabase,
    session: AsyncSession,
    raw_bucket: storage.Bucket,
    sliced_bucket: storage.Bucket,
) -> None:
    source_id: ObjectId = doc["_id"]
    server_id: str = doc["server_id"]
    sensor_map_raw = doc.get("sensor_map") or []
    # DB 저장 형식이 dict {"MAC": "success"} 또는 list ["MAC"] 둘 다 허용
    sensor_map: list[str] = list(sensor_map_raw.keys()) if isinstance(sensor_map_raw, dict) else list(sensor_map_raw)
    timestamp_utc: datetime = doc["timestamp"]
    gcs_path: str = doc.get("gcs_path", "")

    if not sensor_map:
        await db["audio_upload_logs"].update_one(
            {"_id": source_id},
            {"$set": {"slice_status": "failed"}},
        )
        return

    logger.info(f"[slicer] start source_id={source_id} server={server_id} gcs={gcs_path}")

    await db["audio_upload_logs"].update_one(
        {"_id": source_id},
        {"$set": {"slice_status": "processing"}},
    )

    try:
        sensor_lookup = await _build_sensor_lookup(server_id, session)
        logger.info(f"[slicer] sensor_lookup={list(sensor_lookup.keys())}")

        raw_bytes: bytes = await asyncio.to_thread(
            lambda: raw_bucket.blob(gcs_path).download_as_bytes()
        )

        slices = await asyncio.to_thread(
            _split_wav, raw_bytes, sensor_map, sensor_lookup, timestamp_utc, server_id
        )

        await asyncio.to_thread(_upload_slices, slices, raw_bucket, sliced_bucket, gcs_path)

        now = datetime.now(timezone.utc)
        slice_docs = [
            {
                "source_id": source_id,
                "server_id": server_id,
                "sensor_device_id": s["sensor_device_id"],
                "sensor_index": s["sensor_index"],
                "installation_machine": s["installation_machine"],
                "device_name": s["device_name"],
                "timestamp": timestamp_utc,
                "gcs_path": s["gcs_path"],
                "duration_ms": s["duration_ms"],
                "status": "success",
                "processed_at": now,
                "error": None,
            }
            for s in slices
        ]
        await db["audio_slices"].insert_many(slice_docs)

        await db["audio_upload_logs"].update_one(
            {"_id": source_id},
            {"$set": {"slice_status": "done"}},
        )

    except Exception as exc:
        logger.exception(f"[slicer] FAILED source_id={source_id}: {exc}")
        await db["audio_upload_logs"].update_one(
            {"_id": source_id},
            {"$set": {"slice_status": "failed"}},
        )
        raise exc


async def run_slicing_batch(
    db: AsyncIOMotorDatabase,
    session: AsyncSession,
    raw_bucket: storage.Bucket,
    sliced_bucket: storage.Bucket,
    date: str | None = None,
    server_id: str | None = None,
) -> dict:
    """Processes all pending audio_upload_logs for the given date (YYYYMMDD) and optional server_id."""
    if date:
        try:
            day_start = datetime.strptime(date, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError(f"Invalid date format: {date}. Expected YYYYMMDD.")
        # date param is KST — convert to UTC range
        kst_offset = timedelta(hours=9)
        utc_start = day_start - kst_offset
        utc_end = utc_start + timedelta(days=1)
    else:
        # Default: yesterday KST
        yesterday_kst = (datetime.now(KST) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        utc_start = yesterday_kst.astimezone(timezone.utc)
        utc_end = utc_start + timedelta(days=1)

    query: dict = {
        "slice_status": {"$in": ["pending", None]},
        "timestamp": {"$gte": utc_start, "$lt": utc_end},
        "status": "success",
    }
    if server_id:
        query["server_id"] = server_id

    docs = await db["audio_upload_logs"].find(query).to_list(length=None)

    logger.info(f"[batch] date={date} server={server_id} → {len(docs)} docs to process")
    results = {"total": len(docs), "done": 0, "failed": 0}
    for doc in docs:
        try:
            await slice_audio_upload(doc, db, session, raw_bucket, sliced_bucket)
            results["done"] += 1
        except Exception:
            results["failed"] += 1

    return results
