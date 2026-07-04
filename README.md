# Real-Time IoT Analytics Pipeline on TimescaleDB

A self-contained, reproducible demo of a real-time vehicle-telemetry pipeline
built on **TimescaleDB** (PostgreSQL) with a **Grafana** dashboard. A Python
generator simulates a fleet streaming sensor data; TimescaleDB stores it in a
**hypertable** with **columnar compression**, a **continuous aggregate** and a
**retention policy**; Grafana visualizes it live.

Inspired by Timescale's own write-up,
[*Building a Real-Time IoT Analytics Pipeline*](https://medium.com/timescale/building-a-real-time-iot-analytics-pipeline-key-concepts-and-tools-3756cd093724),
scoped to make the **database layer** the star.

## Architecture

```
 ┌──────────────┐     inserts      ┌────────────────────┐     SQL      ┌──────────┐
 │ generator.py │ ───────────────► │    TimescaleDB     │ ───────────► │ Grafana  │
 │ (fleet sim)  │  batched, ~10s   │  hypertable +      │  continuous  │ dashboard│
 └──────────────┘                  │  compression +     │  aggregate   └──────────┘
                                   │  continuous agg +  │
   In production the generator     │  retention policy  │
   would be Kafka → Kafka Connect  └────────────────────┘
```

> **Note on Kafka:** the Timescale article fronts ingestion with Kafka + Kafka
> Connect. That's the right choice at 140k rows/sec in production. This project
> deliberately swaps it for a direct batched writer so the whole stack runs with
> one command and the focus stays on TimescaleDB features — the ingestion path
> is a drop-in replacement.

## TimescaleDB features demonstrated

| Feature | Where | Why it matters |
|---|---|---|
| **Hypertable** (auto time-partitioning) | `schema.sql` | Fast inserts + per-chunk operations |
| **Columnar compression** | `schema.sql` compression policy | 90%+ storage reduction on telemetry |
| **Continuous aggregate** | `telemetry_1min` view | Dashboards never scan raw rows |
| **Retention policy** | `drop_after => 7 days` | Near-instant chunk drops vs. row DELETEs |
| **Execution-plan analysis** | `queries/02_explain_plans.sql` | Prove the aggregate speedup |

## Quick start

Requires Docker + Python 3.9+.

```bash
# 1. Start TimescaleDB + Grafana (schema builds automatically on first boot)
docker compose up -d

# 2. Install the generator's one dependency
pip install -r requirements.txt

# 3. Seed 3h of history, then stream live data
python generator.py --vehicles 50 --rate 500 --backfill-hours 3
```

Open Grafana at **http://localhost:3000** (admin / admin) → the **Fleet
Overview** dashboard is pre-provisioned.

Connect to the DB directly with:
```bash
docker exec -it iot_timescaledb psql -U iot -d iot
```

## What I measured

Measured on this stack with 48,000 readings from a 30-vehicle fleet spanning 4
hours (hourly chunks). Reproduce with the scripts in `queries/`.

| Metric | Result |
|---|---|
| **Columnar compression** | 3952 kB → 1216 kB = **69.2% saved** (3 of 5 chunks compressed) |
| **Query speedup** (last 4h, avg speed/min) | raw hypertable **23.15 ms** → continuous aggregate **1.76 ms** ≈ **13× faster** |
| **Chunks created** | 5 hourly chunks from a 4-hour backfill |

The raw plan does a `ChunkAppend` + `VectorAgg` across every chunk and finalizes
a `HashAggregate` over ~7,800 rows; the continuous aggregate serves the same
answer from pre-materialized 1-minute buckets, skipping the scan entirely.

> Compression ratio grows with data volume and cardinality — at production
> scale (millions of rows/vehicle) Timescale routinely reports 90%+.

## Repo layout

```
docker-compose.yml   TimescaleDB + Grafana
schema.sql           hypertable, compression, continuous aggregate, retention
generator.py         simulated fleet telemetry stream
requirements.txt     psycopg2
queries/             hypertable health, EXPLAIN plans, compression ratio
dashboards/          Grafana dashboard + auto-provisioning
```

## What I learned

> A few sentences on hypertable chunking, why segment-by matters for
> compression, and how continuous aggregates change the query plan. This is the
> section interviewers read — write it in your own words after building.
