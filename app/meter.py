"""ESPHome native-API ingestion.

One ESPHome device = one source. Subscribes to the sensor states we care about and
hands (ts, device, key, value) tuples to a callback. Reconnects with backoff.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import time

from aioesphomeapi import APIClient

from . import core

log = logging.getLogger("loadwatch.meter")


class MeterSource:
    def __init__(self, host: str, port: int, noise_psk: str, on_sample):
        self.host = host
        self.port = port
        self.noise_psk = noise_psk or None
        self.on_sample = on_sample
        self._keymap: dict[int, str] = {}
        self._cli: APIClient | None = None

    # -- state callback ----------------------------------------------------
    def _handle_state(self, state) -> None:
        name = self._keymap.get(getattr(state, "key", None))
        if name is None:
            return
        value = getattr(state, "state", None)
        if getattr(state, "missing_state", False) or value is None:
            log.debug("%s: %s missing", self.host, name)
            return
        try:
            self.on_sample(self.host, name, float(value))
        except (TypeError, ValueError):
            log.warning("%s: non-numeric %s=%r", self.host, name, value)

    # -- discovery ---------------------------------------------------------
    async def _resolve_entities(self) -> list[int]:
        assert self._cli is not None
        entities, _services = await self._cli.list_entities_services()
        available: dict[str, int] = {}
        for ent in entities:
            oid = getattr(ent, "object_id", None)
            key = getattr(ent, "key", None)
            if oid and key is not None:
                available[oid] = key

        wanted: list[int] = []
        for want in core.WATCH:
            key = available.get(want)
            if key is None:  # tolerate firmware variants: match by suffix
                for oid, k in available.items():
                    if oid.endswith(want):
                        key = k
                        break
            if key is None:
                continue
            self._keymap[key] = want
            wanted.append(key)

        missing = sorted(set(core.WATCH) - set(self._keymap.values()))
        log.info(
            "%s: watching %d/%d entities; missing=%s",
            self.host, len(wanted), len(core.WATCH), missing or "none",
        )
        if missing:
            # Self-diagnosing: show the caller every object_id this device exposes.
            log.info("%s: available object_ids: %s", self.host, sorted(available))
        return wanted

    # -- run ---------------------------------------------------------------
    async def run(self, stop: asyncio.Event) -> None:
        backoff = 5
        while not stop.is_set():
            try:
                self._cli = APIClient(
                    self.host, self.port, password="", noise_psk=self.noise_psk
                )
                log.info("%s: connecting to ESPHome API %s:%s", self.host, self.host, self.port)
                await self._cli.connect(login=True)
                log.info("%s: connected", self.host)
                backoff = 5

                wanted = await self._resolve_entities()
                if not wanted:
                    raise RuntimeError("none of the watched entities exist on this device")
                # aioesphomeapi has shipped both sync and async variants of this
                res = self._cli.subscribe_states(self._handle_state)
                if inspect.isawaitable(res):
                    await res
                log.info("%s: subscribed to %d state keys", self.host, len(wanted))

                while not stop.is_set():
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - stay alive on any transport error
                log.warning("%s: connection problem: %s (retry in %ss)", self.host, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)
            finally:
                if self._cli is not None:
                    try:
                        await self._cli.disconnect()
                    except Exception:  # noqa: BLE001
                        pass
                    self._cli = None
        log.info("%s: stopped", self.host)


class MeterHub:
    """Collects samples from every configured source into one flush buffer."""

    def __init__(self, hosts: list[str], port: int, noise_psk: str):
        self.hosts = hosts
        self.port = port
        self.noise_psk = noise_psk
        self.buffer: list[tuple] = []
        self.latest: dict[str, float] = {}
        self._last: dict[str, float] = {}
        # optional extra consumer (the detector), set after construction.
        # Kept as an attribute and routed through _dispatch so assigning it
        # later actually takes effect (sources hold _dispatch, not the hook).
        self.hook = None
        self.sources = [
            MeterSource(h, port, noise_psk, self._dispatch) for h in hosts
        ]

    def _dispatch(self, device: str, key: str, value: float) -> None:
        self._on_sample(device, key, value)
        if self.hook is not None:
            self.hook(device, key, value)

    def _on_sample(self, device: str, key: str, value: float) -> None:
        now = time.time()
        self.buffer.append((now, device, key, value))
        if device == self.hosts[0]:
            self.latest[key] = value
            self._last[key] = now

    async def run(self, stop: asyncio.Event) -> None:
        await asyncio.gather(*(s.run(stop) for s in self.sources))
