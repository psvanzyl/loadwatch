-- loadwatch schema v1
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw 1 Hz samples. Narrow by design: extra sources (smart plug, later a second
-- meter) are just another (device, key) pair - no schema change.
CREATE TABLE IF NOT EXISTS samples (
    ts    timestamptz      NOT NULL,
    device text            NOT NULL,
    key   text             NOT NULL,
    value double precision NOT NULL
);

SELECT create_hypertable('samples', 'ts', chunk_time_interval => interval '1 day',
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS samples_device_key_ts_idx ON samples (device, key, ts DESC);

-- 1-minute rollup for charts and long-term retention.
CREATE MATERIALIZED VIEW IF NOT EXISTS samples_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', ts) AS bucket,
       device,
       key,
       avg(value)  AS avg,
       min(value)  AS min,
       max(value)  AS max,
       last(value, ts) AS last
FROM samples
GROUP BY bucket, device, key;

SELECT add_continuous_aggregate_policy('samples_1m',
    start_offset      => interval '3 hours',
    end_offset        => interval '1 minute',
    schedule_interval => interval '1 minute',
    if_not_exists     => TRUE);

-- Compress raw samples after 7 days (keeps the full 1 Hz history, cheaply).
ALTER TABLE samples SET (timescaledb.compress,
                         timescaledb.compress_segmentby = 'device,key',
                         timescaledb.compress_orderby   = 'ts DESC');
SELECT add_compression_policy('samples', interval '7 days', if_not_exists => TRUE);

-- Load episodes detected from the aggregate power stream.
CREATE TABLE IF NOT EXISTS events (
    id           bigserial PRIMARY KEY,
    ts_start     timestamptz NOT NULL,
    ts_end       timestamptz,
    direction    smallint    NOT NULL,   -- +1 = load turned on, -1 = off
    delta_w      double precision,       -- the step size
    peak_w       double precision,       -- peak power during the episode
    baseline_w   double precision,       -- baseline the step came off
    duration_s   double precision,
    energy_wh    double precision,       -- integral of (P - baseline) over the episode
    appliance    text,                   -- label: null until labelled
    label_source text,                   -- manual | plug | model
    labeled_at   timestamptz,
    notes        text
);

CREATE INDEX IF NOT EXISTS events_ts_start_idx ON events (ts_start DESC);
CREATE INDEX IF NOT EXISTS events_appliance_idx ON events (appliance);

CREATE TABLE IF NOT EXISTS appliances (
    name     text PRIMARY KEY,
    notes    text,
    is_large boolean NOT NULL DEFAULT TRUE
);

INSERT INTO appliances (name, notes) VALUES
    ('kettle',         'water kettle / boiler, ~2 kW step'),
    ('oven',           'electric oven, 2-3 kW, thermostatic cycling'),
    ('microwave',      '~1.2 kW with ~50% duty cycle'),
    ('dishwasher',     'heater cycling ~1-2 kW'),
    ('washing_machine','heater ~2 kW, motor ~500 W'),
    ('dryer',          '~2-3 kW heater cycling'),
    ('fridge',         'compressor ~80-150 W cycling'),
    ('freezer',        'compressor ~80-150 W cycling'),
    ('iron',           '~1-2 kW, duty-cycled'),
    ('vacuum',         '~1-1.5 kW steady'),
    ('water_boiler',   'hot water / boiler'),
    ('ev_charger',     'EV charge point'),
    ('heater',         'space heater'),
    ('unknown_large',  'clearly a big load, unidentifiable'),
    ('ignore',         'noise / not a real load')
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS meta (
    k text PRIMARY KEY,
    v text
);
