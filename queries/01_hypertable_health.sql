-- Hypertable overview: chunks, ranges and size on disk.
-- The first thing you check when triaging a TimescaleDB performance ticket.

-- How many chunks, and the hypertable's total size:
SELECT hypertable_name,
       count(*)                                   AS num_chunks,
       pg_size_pretty(sum(total_bytes))           AS total_size
FROM timescaledb_information.chunks c
JOIN timescaledb_information.hypertables h USING (hypertable_name)
CROSS JOIN LATERAL (
    SELECT (chunk_detailed_size).total_bytes
    FROM chunk_detailed_size(format('%I.%I', c.chunk_schema, c.chunk_name))
) sz
WHERE hypertable_name = 'vehicle_telemetry'
GROUP BY hypertable_name;

-- Per-chunk detail incl. whether each chunk is compressed:
SELECT chunk_name,
       range_start,
       range_end,
       is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'vehicle_telemetry'
ORDER BY range_start DESC;
