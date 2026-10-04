"""Loadwatch API + labelling UI."""
from __future__ import annotations

import csv
import io
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from . import core

log = logging.getLogger("loadwatch.api")
STATIC = Path(__file__).parent / "static"

# When served behind a Traefik sub-path (https://host/loadwatch) with a
# stripprefix middleware, the app still serves "/" - but every URL it EMITS must
# carry the prefix, because the browser requests the prefixed path.
# Traefik's StripPrefix sets X-Forwarded-Prefix, so the same image works both
# standalone (no header) and behind the portal path.
PREFIX = os.environ.get("PORTAL_PREFIX", "").rstrip("/")

_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _pool
    core.setup_logging()
    _pool = await core.make_pool()
    log.info("api up")
    try:
        yield
    finally:
        if _pool is not None:
            await _pool.close()


app = FastAPI(title="loadwatch", version="0.1.0", lifespan=lifespan)


async def auth(authorization: str | None = Header(default=None)) -> None:
    token = core.API_TOKEN
    if not token:
        return
    if authorization != f"Bearer {token}":
        raise HTTPException(status_code=401, detail="bad or missing token")


# --------------------------------------------------------------------- pages
@app.get("/")
async def index(request: Request) -> HTMLResponse:
    prefix = request.headers.get("x-forwarded-prefix", PREFIX).rstrip("/")
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    return HTMLResponse(html.replace("__PREFIX__", prefix))


# ----------------------------------------------------------------------- api
@app.get("/api/status", dependencies=[Depends(auth)])
async def status():
    async with _pool.acquire() as con:
        latest = await con.fetch(
            """
            SELECT DISTINCT ON (key) key, value, ts
            FROM samples WHERE device = $1 ORDER BY key, ts DESC
            """,
            core.METER_HOSTS[0],
        )
        today = await con.fetchrow(
            """
            SELECT
              (SELECT value FROM samples WHERE key='energy_consumed_tariff_1'
                 ORDER BY ts DESC LIMIT 1) AS e_cons_t1,
              (SELECT value FROM samples WHERE key='energy_consumed_tariff_2'
                 ORDER BY ts DESC LIMIT 1) AS e_cons_t2,
              (SELECT value FROM samples WHERE key='energy_produced_tariff_1'
                 ORDER BY ts DESC LIMIT 1) AS e_prod_t1,
              (SELECT value FROM samples WHERE key='energy_produced_tariff_2'
                 ORDER BY ts DESC LIMIT 1) AS e_prod_t2
            """
        )
        count = await con.fetchval("SELECT count(*) FROM events")
        labelled = await con.fetchval("SELECT count(*) FROM events WHERE appliance IS NOT NULL")
    return {
        "device": core.METER_HOSTS[0],
        "latest": {r["key"]: {"value": r["value"], "ts": r["ts"].isoformat()} for r in latest},
        "counters": dict(today) if today else {},
        "events": {"total": count, "labelled": labelled},
    }


@app.get("/api/power", dependencies=[Depends(auth)])
async def power(hours: float = Query(6, ge=0.05, le=24 * 14),
                key: str = Query(core.POWER_KEY)):
    """Downsampled series from the 1-minute continuous aggregate."""
    if hours <= 3:
        src = "samples"
        bucket = "30 seconds"
    else:
        src = "samples_1m"
        bucket = "1 minute"
    window = min(hours, 6) if src == "samples" else hours
    async with _pool.acquire() as con:
        rows = await con.fetch(
            f"""
            SELECT time_bucket($1::interval, ts) AS bucket,
                   avg(value) AS avg, max(value) AS max, min(value) AS min
            FROM {src}
            WHERE key = $2 AND device = $3 AND ts > now() - ($4::float * interval '1 hour')
            GROUP BY bucket ORDER BY bucket
            """,
            bucket, key, core.METER_HOSTS[0], window,
        )
    return {
        "key": key,
        "bucket": bucket,
        "points": [
            {"t": r["bucket"].isoformat(), "avg": r["avg"], "max": r["max"], "min": r["min"]}
            for r in rows
        ],
    }


@app.get("/api/events", dependencies=[Depends(auth)])
async def events(limit: int = Query(100, ge=1, le=1000), unlabelled: bool = False):
    q = "SELECT * FROM events"
    if unlabelled:
        q += " WHERE appliance IS NULL"
    q += " ORDER BY ts_start DESC LIMIT $1"
    async with _pool.acquire() as con:
        rows = await con.fetch(q, limit)
        apps = await con.fetch("SELECT name, notes, is_large FROM appliances ORDER BY name")
    return {
        "events": [dict(r) for r in rows],
        "appliances": [dict(a) for a in apps],
    }


@app.post("/api/events/{event_id}/label", dependencies=[Depends(auth)])
async def label(event_id: int, appliance: str = Query(...), notes: str | None = None):
    async with _pool.acquire() as con:
        ok = await con.fetchval(
            "SELECT 1 FROM appliances WHERE name = $1", appliance
        )
        if not ok:
            raise HTTPException(400, f"unknown appliance {appliance!r}")
        row = await con.fetchrow(
            """
            UPDATE events
               SET appliance = $2, label_source = 'manual', labeled_at = now(),
                   notes = COALESCE($3, notes)
             WHERE id = $1
            RETURNING id, appliance, labeled_at
            """,
            event_id, appliance, notes,
        )
    if row is None:
        raise HTTPException(404, "no such event")
    return dict(row)


@app.get("/api/events/{event_id}/window", dependencies=[Depends(auth)])
async def event_window(event_id: int, pad_s: float = 60):
    """Raw 1 Hz trace around an event - the training input for that label."""
    async with _pool.acquire() as con:
        ev = await con.fetchrow("SELECT * FROM events WHERE id = $1", event_id)
        if ev is None:
            raise HTTPException(404, "no such event")
        rows = await con.fetch(
            """
            SELECT ts, value FROM samples
             WHERE key = $1 AND device = $2
               AND ts BETWEEN $3 - ($4::float * interval '1 second')
                          AND COALESCE($5, $3) + ($4::float * interval '1 second')
             ORDER BY ts
            """,
            core.POWER_KEY, core.METER_HOSTS[0], ev["ts_start"], pad_s, ev["ts_end"],
        )
    return {
        "event": dict(ev),
        "samples": [{"t": r["ts"].isoformat(), "w": r["value"]} for r in rows],
    }


@app.get("/api/export.csv", dependencies=[Depends(auth)])
async def export_csv(days: float = Query(30, ge=0.1, le=3650)):
    """Labelled events, one row each - the training set index."""
    async with _pool.acquire() as con:
        rows = await con.fetch(
            """
            SELECT id, ts_start, ts_end, direction, delta_w, peak_w, baseline_w,
                   duration_s, energy_wh, appliance, label_source
            FROM events
            WHERE ts_start > now() - ($1::float * interval '1 day')
            ORDER BY ts_start
            """,
            days,
        )
    buf = io.StringIO()
    if rows:
        w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=loadwatch_events.csv"},
    )


@app.get("/api/appliances", dependencies=[Depends(auth)])
async def appliances():
    async with _pool.acquire() as con:
        rows = await con.fetch("SELECT name, notes, is_large FROM appliances ORDER BY name")
    return [dict(r) for r in rows]
