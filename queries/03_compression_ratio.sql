-- Measure real compression savings once chunks have aged past the 1h policy
-- (or force it: SELECT compress_chunk(c) FROM show_chunks('vehicle_telemetry') c;)

SELECT
    pg_size_pretty(before_compression_total_bytes) AS before,
    pg_size_pretty(after_compression_total_bytes)  AS after,
    round(100.0 * (1 - after_compression_total_bytes::numeric
                       / nullif(before_compression_total_bytes, 0)), 1) AS pct_saved
FROM hypertable_compression_stats('vehicle_telemetry');
