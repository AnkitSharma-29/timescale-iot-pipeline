-- Reading execution plans is a MUST-HAVE skill for this role. These two
-- queries answer the same question ("avg speed per minute for the last hour")
-- against the raw hypertable vs. the continuous aggregate. Run both with
-- EXPLAIN ANALYZE and compare the plan + timing.

-- (a) Raw scan + on-the-fly aggregation:
EXPLAIN (ANALYZE, BUFFERS)
SELECT time_bucket('1 minute', time) AS bucket,
       vehicle_id,
       avg(speed_kph) AS avg_speed
FROM vehicle_telemetry
WHERE time > now() - INTERVAL '1 hour'
GROUP BY bucket, vehicle_id
ORDER BY bucket DESC;

-- (b) Same answer, served from the pre-computed continuous aggregate:
EXPLAIN (ANALYZE, BUFFERS)
SELECT bucket, vehicle_id, avg_speed
FROM telemetry_1min
WHERE bucket > now() - INTERVAL '1 hour'
ORDER BY bucket DESC;

-- What to look for: (b) touches far fewer rows/buffers and skips the aggregation
-- node entirely. Note the actual times in the README "What I measured" section.
