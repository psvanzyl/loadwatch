"""Step-change / episode detection on a 1 Hz real-power stream.

Model: the house has a running baseline (everything always-on). A load *episode*
starts when instantaneous power steps away from that baseline by more than
`threshold_w` and stays there for `min_duration_s`; it ends when power returns
within `close_band_w` of the baseline and holds for `close_hold_s`.

The baseline is frozen while an episode is open, so a second load switching on
mid-episode does not corrupt the first one's delta.
"""
from __future__ import annotations

import statistics
from collections import deque
from dataclasses import dataclass

from . import core


@dataclass
class Event:
    ts_start: float
    ts_end: float | None = None
    direction: int = 1
    delta_w: float = 0.0
    peak_w: float = 0.0
    baseline_w: float = 0.0
    duration_s: float = 0.0
    energy_wh: float = 0.0

    # helpers for the DB layer (epoch -> aware datetime)
    @property
    def ts_start_dt(self):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(self.ts_start, tz=timezone.utc)

    @property
    def ts_end_dt(self):
        from datetime import datetime, timezone
        if self.ts_end is None:
            return None
        return datetime.fromtimestamp(self.ts_end, tz=timezone.utc)


class EventDetector:
    def __init__(self, threshold_w=None, min_duration_s=None, close_band_w=None,
                 close_hold_s=None, baseline_window_s=None, max_event_s=None):
        self.threshold_w = core.EVENT_THRESHOLD_W if threshold_w is None else threshold_w
        self.min_duration_s = core.EVENT_MIN_DURATION_S if min_duration_s is None else min_duration_s
        self.close_band_w = core.EVENT_CLOSE_BAND_W if close_band_w is None else close_band_w
        self.close_hold_s = core.EVENT_CLOSE_HOLD_S if close_hold_s is None else close_hold_s
        self.baseline_window_s = core.BASELINE_WINDOW_S if baseline_window_s is None else baseline_window_s
        self.max_event_s = core.EVENT_MAX_S if max_event_s is None else max_event_s

        self._hist: deque[tuple[float, float]] = deque()   # (ts, w) since last event
        self._cand: tuple[float, float] | None = None      # candidate step
        self._ev: Event | None = None
        self._back_since: float | None = None
        self._last_ts: float | None = None
        self._energy_acc = 0.0

    # ------------------------------------------------------------------ util
    def _baseline(self) -> float | None:
        if not self._hist:
            return None
        cutoff = (self._last_ts or 0.0) - self.baseline_window_s
        window = [w for ts, w in self._hist if ts >= cutoff]
        if len(window) < 3:
            return None
        return statistics.median(window)

    # ------------------------------------------------------------------- fed
    def add(self, ts: float, w: float) -> Event | None:
        """Feed one sample. Returns a completed Event when one closes."""
        self._last_ts = ts
        completed: Event | None = None

        if self._ev is None:
            base = self._baseline()
            self._hist.append((ts, w))
            # keep memory bounded
            if len(self._hist) > 10000:
                self._hist.popleft()

            if base is not None:
                if abs(w - base) > self.threshold_w:
                    if self._cand is None:
                        self._cand = (ts, w)
                    elif ts - self._cand[0] >= self.min_duration_s:
                        # episode opens retroactively at the candidate time
                        delta = w - base
                        self._ev = Event(
                            ts_start=self._cand[0],
                            direction=1 if delta > 0 else -1,
                            delta_w=delta,
                            peak_w=w,
                            baseline_w=base,
                        )
                        self._energy_acc = 0.0
                        self._back_since = None
                        self._cand = None
                        self._hist.clear()
                        self._hist.append((ts, w))
                else:
                    self._cand = None
        else:
            ev = self._ev
            dev = w - ev.baseline_w
            if abs(w) > abs(ev.peak_w):
                ev.peak_w = w
            # energy above baseline, trapezoid-ish (1 Hz => dt of one step)
            dt = max(0.0, ts - (self._last_ts_prev or ts))
            self._energy_acc += max(0.0, dev) * dt / 3600.0
            self._hist.append((ts, w))

            if abs(dev) < self.close_band_w:
                if self._back_since is None:
                    self._back_since = ts
                elif ts - self._back_since >= self.close_hold_s:
                    ev.ts_end = self._back_since
                    ev.duration_s = ev.ts_end - ev.ts_start
                    ev.energy_wh = self._energy_acc
                    completed = ev
                    self._ev = None
                    self._back_since = None
                    self._cand = None
                    self._hist.clear()
            else:
                self._back_since = None

            if self._ev is not None and ts - self._ev.ts_start >= self.max_event_s:
                self._ev.ts_end = ts
                self._ev.duration_s = ts - self._ev.ts_start
                self._ev.energy_wh = self._energy_acc
                completed = self._ev
                self._ev = None
                self._back_since = None
                self._hist.clear()

        self._last_ts_prev = ts
        return completed

    # for the live status endpoint
    @property
    def active(self) -> Event | None:
        return self._ev

    _last_ts_prev: float | None = None

    def to_rows(self, ev: Event) -> tuple:
        return (ev.ts_start_dt, ev.ts_end_dt, ev.direction, ev.delta_w, ev.peak_w,
                ev.baseline_w, ev.duration_s, ev.energy_wh)
