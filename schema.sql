-- =============================================================================
-- Real-time IoT vehicle-telemetry schema for TimescaleDB
--
-- Demonstrates the five TimescaleDB features that matter for real-time
-- analytics: hypertables, columnar compression, continuous aggregates,
-- time partitioning (automatic chunking) and data retention.
--
-- This file runs automatically on first container boot via
-- /docker-entrypoint-initdb.d. Every block is commented so the design
-- decisions are explicit.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -----------------------------------------------------------------------------
-- 1. Raw telemetry table
-- -----------------------------------------------------------------------------
-- One row per sensor reading emitted by a vehicle. This is the "wide" raw
-- landing table; everything downstream is derived from it.
CREATE TABLE vehicle_telemetry (
    time        TIMESTAMPTZ       NOT NULL,
    vehicle_id  INTEGER           NOT NULL,
    speed_kph   DOUBLE PRECISION,          -- current speed
    fuel_pct    DOUBLE PRECISION,          -- remaining fuel %
    engine_temp DOUBLE PRECISION,          -- engine temperature (C)
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION
);

-- Turn it into a hypertable partitioned on time. We chunk per hour: small
-- chunks keep inserts fast and let older chunks be compressed / dropped
-- individually. (Pick a chunk interval so a chunk holds ~1-7 days of data in
-- production; hourly here keeps the demo's compression story visible fast.)
SELECT create_hypertable('vehicle_telemetry', by_range('time', INTERVAL '1 hour'));

-- Index for the most common access pattern: "latest readings for a vehicle".
CREATE INDEX ix_vehicle_time ON vehicle_telemetry (vehicle_id, time DESC);

-- -----------------------------------------------------------------------------
-- 2. Columnar compression
-- -----------------------------------------------------------------------------
-- Compress chunks older than 1 hour. Segment by vehicle_id so per-vehicle
-- scans stay fast; order by time so recent-within-chunk reads are sequential.
-- On telemetry like this, compression typically reaches 90%+ (measure it with
-- queries/03_compression_ratio.sql after data has aged).
ALTER TABLE vehicle_telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vehicle_id',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('vehicle_telemetry', compress_after => INTERVAL '1 hour');

-- -----------------------------------------------------------------------------
-- 3. Continuous aggregate: per-minute rollup
-- -----------------------------------------------------------------------------
-- Pre-computes 1-minute summaries so dashboards never scan raw rows. This is
-- the single biggest query-speed win for real-time analytics.
CREATE MATERIALIZED VIEW telemetry_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    vehicle_id,
    avg(speed_kph)   AS avg_speed,
    max(speed_kph)   AS max_speed,
    avg(engine_temp) AS avg_engine_temp,
    min(fuel_pct)    AS min_fuel_pct,
    count(*)         AS reading_count
FROM vehicle_telemetry
GROUP BY bucket, vehicle_id
WITH NO DATA;

-- Keep the aggregate fresh. The refresh window (start_offset - end_offset) must
-- span at least two buckets, so with 1-minute buckets we use 10 min -> 1 min.
SELECT add_continuous_aggregate_policy('telemetry_1min',
    start_offset      => INTERVAL '10 minutes',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute');

-- -----------------------------------------------------------------------------
-- 4. Retention policy
-- -----------------------------------------------------------------------------
-- Drop raw readings older than 7 days (dashboards keep working off the
-- continuous aggregate). Chunk-based drops are near-instant vs. row DELETEs.
SELECT add_retention_policy('vehicle_telemetry', drop_after => INTERVAL '7 days');
