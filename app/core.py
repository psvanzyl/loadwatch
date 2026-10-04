"""Config + database helpers."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import asyncpg

log = logging.getLogger("loadwatch")


def env_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ[name])
    except (KeyError, TypeError, ValueError):
        return default


DATABASE_URL = env_str(
    "DATABASE_URL", "postgresql://loadwatch:loadwatch@db:5432/loadwatch"
)

# --- meter / sources ------------------------------------------------------
METER_HOSTS = [h.strip() for h in env_str("METER_HOSTS", "192.168.178.213").split(",") if h.strip()]
METER_PORT = int(env_float("METER_PORT", 6053))
METER_NOISE_PSK = env_str("METER_NOISE_PSK", "")

# ESPHome object_id -> our column/key name. Matched by object_id first, then by
# suffix (the Zuidwijk firmware keeps the plain dsmr names).
WATCH = {
    "power_consumed_phase_1": "power_consumed_phase_1",
    "power_produced_phase_1": "power_produced_phase_1",
    "voltage_phase_1": "voltage_phase_1",
    "current_phase_1": "current_phase_1",
    "energy_consumed_tariff_1": "energy_consumed_tariff_1",
    "energy_consumed_tariff_2": "energy_consumed_tariff_2",
    "energy_produced_tariff_1": "energy_produced_tariff_1",
    "energy_produced_tariff_2": "energy_produced_tariff_2",
    "gas_consumed": "gas_consumed",
}

POWER_KEY = "power_consumed_phase_1"   # the aggregate signal the detector watches

# --- detection ------------------------------------------------------------
EVENT_THRESHOLD_W = env_float("EVENT_THRESHOLD_W", 80.0)
EVENT_MIN_DURATION_S = env_float("EVENT_MIN_DURATION_S", 3.0)
EVENT_CLOSE_BAND_W = env_float("EVENT_CLOSE_BAND_W", 40.0)
EVENT_CLOSE_HOLD_S = env_float("EVENT_CLOSE_HOLD_S", 3.0)
EVENT_MAX_S = env_float("EVENT_MAX_S", 4 * 3600.0)

API_TOKEN = env_str("API_TOKEN", "")

FLUSH_INTERVAL_S = env_float("FLUSH_INTERVAL_S", 5.0)
BASELINE_WINDOW_S = env_float("BASELINE_WINDOW_S", 60.0)


def setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


async def make_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)


async def insert_samples(pool: asyncpg.Pool, rows: list[tuple]) -> None:
    """rows: (ts, device, key, value). ts may be an epoch float or a datetime."""
    if not rows:
        return
    norm = [
        (
            ts if not isinstance(ts, (int, float))
            else datetime.fromtimestamp(float(ts), tz=timezone.utc),
            device,
            key,
            float(value),
        )
        for ts, device, key, value in rows
    ]
    async with pool.acquire() as con:
        await con.executemany(
            "INSERT INTO samples (ts, device, key, value) VALUES ($1, $2, $3, $4)", norm
        )


async def insert_event(pool: asyncpg.Pool, ev) -> int:
    async with pool.acquire() as con:
        return await con.fetchval(
            """
            INSERT INTO events (ts_start, ts_end, direction, delta_w, peak_w,
                                baseline_w, duration_s, energy_wh)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            """,
            ev.ts_start_dt, ev.ts_end_dt, ev.direction, ev.delta_w, ev.peak_w,
            ev.baseline_w, ev.duration_s, ev.energy_wh,
        )
