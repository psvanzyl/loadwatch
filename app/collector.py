"""Collector: ingest the P1 stream, store it, detect load episodes."""
from __future__ import annotations

import asyncio
import logging
import signal
import time

from . import core
from .detector import EventDetector
from .meter import MeterHub

log = logging.getLogger("loadwatch.collector")


async def amain() -> None:
    core.setup_logging()
    pool = await core.make_pool()
    log.info("db pool ready")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    hub = MeterHub(core.METER_HOSTS, core.METER_PORT, core.METER_NOISE_PSK)
    detector = EventDetector()
    log.info(
        "detector: threshold=%.0fW min_duration=%.0fs close_band=%.0fW hold=%.0fs",
        detector.threshold_w, detector.min_duration_s, detector.close_band_w,
        detector.close_hold_s,
    )

    # everything is stored; the aggregate power also feeds the detector
    pending_events: list = []

    def hook(device: str, key: str, value: float) -> None:
        if device == core.METER_HOSTS[0] and key == core.POWER_KEY:
            ev = detector.add(time.time(), value)
            if ev is not None and ev.duration_s >= detector.min_duration_s:
                pending_events.append(ev)

    hub.hook = hook

    async def flusher() -> None:
        while not stop.is_set():
            try:
                await asyncio.sleep(core.FLUSH_INTERVAL_S)
                rows, hub.buffer = hub.buffer, []
                await core.insert_samples(pool, rows)
                while pending_events:
                    ev = pending_events.pop(0)
                    eid = await core.insert_event(pool, ev)
                    log.info(
                        "event #%s %s %.0fW dur=%.0fs energy=%.1fWh",
                        eid, "ON" if ev.direction > 0 else "OFF", ev.delta_w,
                        ev.duration_s, ev.energy_wh,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("flush failed: %s", exc)

    tasks = [
        asyncio.create_task(hub.run(stop), name="meter"),
        asyncio.create_task(flusher(), name="flusher"),
        asyncio.create_task(stop.wait(), name="stop"),
    ]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    for t in done:
        exc = t.exception()
        if exc:
            log.error("task %s died: %s", t.get_name(), exc)

    # final flush
    rows, hub.buffer = hub.buffer, []
    await core.insert_samples(pool, rows)
    await pool.close()
    log.info("collector stopped")


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
