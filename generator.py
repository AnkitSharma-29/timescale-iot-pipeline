"""
Simulated real-time IoT ingestion for the TimescaleDB pipeline.

Emits vehicle telemetry for a fleet of N vehicles, batching inserts the way a
Kafka Connect sink would. In a production stack this process would be replaced
by Kafka -> Kafka Connect JDBC sink; here it stands in so the whole pipeline
runs with a single `python generator.py`.

Usage:
    pip install -r requirements.txt
    python generator.py --vehicles 50 --rate 500        # live stream
    python generator.py --backfill-hours 3              # seed history first
"""
import argparse
import math
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values

DSN = "host=localhost port=5432 dbname=iot user=iot password=iotpass"

INSERT_SQL = """
    INSERT INTO vehicle_telemetry
        (time, vehicle_id, speed_kph, fuel_pct, engine_temp, latitude, longitude)
    VALUES %s
"""


class Vehicle:
    """Holds per-vehicle state so readings drift realistically over time."""

    def __init__(self, vid):
        self.vid = vid
        self.speed = random.uniform(20, 80)
        self.fuel = random.uniform(40, 100)
        self.engine_temp = random.uniform(80, 95)
        self.lat = random.uniform(12.90, 13.10)   # ~Bengaluru bounding box
        self.lon = random.uniform(77.50, 77.70)

    def tick(self):
        # Random-walk each metric within believable bounds.
        self.speed = max(0, min(120, self.speed + random.uniform(-8, 8)))
        self.fuel = max(0, self.fuel - random.uniform(0, 0.05))
        self.engine_temp = max(70, min(120, self.engine_temp + random.uniform(-1.5, 1.5)))
        self.lat += random.uniform(-0.0008, 0.0008)
        self.lon += random.uniform(-0.0008, 0.0008)

    def row(self, ts):
        return (ts, self.vid, round(self.speed, 1), round(self.fuel, 2),
                round(self.engine_temp, 1), round(self.lat, 6), round(self.lon, 6))


def connect():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    return conn


def backfill(conn, vehicles, hours):
    """Seed historical rows so compression / aggregates have data to work on."""
    print(f"Backfilling {hours}h of history for {len(vehicles)} vehicles...")
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)
    batch, total = [], 0
    ts = start
    while ts < now:
        for v in vehicles:
            v.tick()
            batch.append(v.row(ts))
        if len(batch) >= 5000:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, batch)
            total += len(batch)
            batch = []
        ts += timedelta(seconds=10)  # one reading per vehicle every 10s
    if batch:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, batch)
        total += len(batch)
    print(f"Backfill done: {total:,} rows.")


def stream(conn, vehicles, rate):
    """Continuously emit ~`rate` rows/sec until interrupted."""
    print(f"Streaming ~{rate} rows/sec for {len(vehicles)} vehicles. Ctrl-C to stop.")
    interval = len(vehicles) / rate  # seconds between full fleet sweeps
    emitted = 0
    t0 = time.time()
    try:
        while True:
            ts = datetime.now(timezone.utc)
            batch = []
            for v in vehicles:
                v.tick()
                batch.append(v.row(ts))
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, batch)
            emitted += len(batch)
            if emitted % (rate * 10) < len(vehicles):
                elapsed = time.time() - t0
                print(f"  {emitted:,} rows | {emitted/elapsed:,.0f} rows/sec")
            time.sleep(max(0, interval))
    except KeyboardInterrupt:
        print(f"\nStopped. Emitted {emitted:,} rows.")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--vehicles", type=int, default=50)
    p.add_argument("--rate", type=int, default=500, help="target rows/sec while streaming")
    p.add_argument("--backfill-hours", type=int, default=0,
                   help="seed this many hours of history, then stream")
    args = p.parse_args()

    fleet = [Vehicle(i) for i in range(1, args.vehicles + 1)]
    conn = connect()
    if args.backfill_hours:
        backfill(conn, fleet, args.backfill_hours)
    stream(conn, fleet, args.rate)


if __name__ == "__main__":
    main()
