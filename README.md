# loadwatch — Tier-0 NILM dataset builder

Non-intrusive load monitoring, working from a **single Dutch DSMR-5 P1 stream at 1 Hz**.
Goal: build a *labelled* dataset of large-load switching events, so a model can later
identify which big appliance turned on.

## What this is

| Layer | What it does |
|---|---|
| `collector` | Subscribes to the SlimmeLezer+ **ESPHome native API** (1 Hz) and stores every sample |
| `detector` | Step-change detection → **load episodes** (on-step, duration, energy, peak) |
| `api` + UI | Live power, event list, **one-click labelling**, CSV export for training |
| `db` | TimescaleDB (Postgres 16) — 1-min continuous aggregate, compression after 7 days |

No Home Assistant involvement on the ingest path (HA stays the notification shell).

## Architecture decision (2026-10-04)

Separate service on **dev CT141**, *not* inside HAOS:

- HA here is **Core-in-Docker** — custom Python services installed there die on every
  HA upgrade, and the recorder is not a dataset store (purges raw state ~10 days, writes
  on-change).
- Ingest goes **straight to the meter** (`192.168.178.213:6053`, ESPHome API, no
  encryption) — no HA token, no secrets on this path.
- Public URL via Coolify: **`loadwatch.xm2561.duckdns.org`** (the `xm2561` *apex* belongs
  to Headscale — never take it).

## Data model

```sql
samples(ts timestamptz, device text, key text, value double precision)  -- hypertable, 1 day chunks
events(id, ts_start, ts_end, direction, delta_w, peak_w, baseline_w, duration_s,
       energy_wh, appliance, label_source, labeled_at, notes)
appliances(name, notes, is_large)
```

`samples` is **narrow** (device, key) so additional sources — a smart plug, later a
second meter — need no schema change.

## Run it (dev)

```bash
cd /root/projects/loadwatch
cp .env.example .env         # set API_TOKEN
docker compose up -d --build
docker compose logs -f collector
```

UI: `http://192.168.178.20:8099/` (token = `API_TOKEN`).

## Collector entity keys

The collector matches ESPHome `object_id`s, discovered at connect time and logged:

| object_id | meaning |
|---|---|
| `power_consumed_phase_1` | instantaneous import W |
| `power_produced_phase_1` | instantaneous export W |
| `voltage_phase_1`, `current_phase_1` | grid V / A |
| `energy_consumed_tariff_1/2` | import kWh counters |
| `energy_produced_tariff_1/2` | export kWh counters |
| `gas_consumed` | gas m³ (M-Bus, ~5 min) |

If a key is missing the collector logs **every** object_id it can see — that is the
discovery mechanism, not a guess.

## Adding the smart plug (ground truth)

Add its host to `METER_HOSTS` (comma-separated) — samples land with `device = <host>`.
A plug on one appliance gives labelled ground truth for exactly that appliance, which is
what a supervised model needs; the P1 event stream covers everything else.

## Export for training

`GET /api/export.csv?days=30` → one row per labelled event plus its raw power window
(`GET /api/events/{id}/window` returns the ±60 s raw trace for that event).

## Detection defaults

`EVENT_THRESHOLD_W=80` (step size), `EVENT_MIN_DURATION_S=3`, close back within 40 W held
3 s. Night baseload is ~50–200 W here; a kettle/oven/washer step is 1000–2500 W.
Documented limitation: two loads stepping at the same instant inside one open episode
are seen as one event.

## Roadmap

1. ✅ capture + detect + label (this repo)
2. plug lane → labelled ground truth
3. HA REST sensor → "big load started" notifications
4. classifier trained on the labelled events (`trainer/`)
