"""
UrbanPulse - Bus GPS Producer
=============================
Simulates ~2,400 events/sec of GPS pings from 12,000 buses.

Ordering guarantee: every message is keyed on `route_id`. Kafka guarantees
that all messages sharing a key land on the same partition and are appended
in send order, so any consumer reading a single partition sees that route's
bus positions strictly in the order they were produced - which is exactly
what the downstream Kafka Streams enrichment / bus-bunching detector needs.

Delivery semantics: idempotent producer (acks=all, enable.idempotence=true)
prevents duplicate/out-of-order writes on retried sends, which matters here
because ordering-per-key is the whole point of this producer.
"""
import json
import logging
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from common_config import BOOTSTRAP_SERVERS, TOPIC_BUS_GPS, ROUTES, BUSES_PER_ROUTE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bus_gps_producer")

# MetroConnect bounding box (approx.) used for simulated lat/lon walk.
LAT_MIN, LAT_MAX = 12.85, 13.10
LON_MIN, LON_MAX = 77.45, 77.75

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "enable.idempotence": True,     # exactly-once per partition on retry
    "retries": 10,
    "linger.ms": 5,                 # small batching window for throughput
    "compression.type": "snappy",
}

producer = Producer(producer_conf)

# In-memory per-bus state so positions drift realistically instead of
# teleporting between random points on every tick.
bus_state = {}
for route in ROUTES:
    for b in range(BUSES_PER_ROUTE):
        bus_id = f"{route}-BUS-{b:03d}"
        bus_state[bus_id] = {
            "route_id": route,
            "lat": random.uniform(LAT_MIN, LAT_MAX),
            "lon": random.uniform(LON_MIN, LON_MAX),
            "speed_kmh": random.uniform(15, 45),
        }


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for route=%s: %s", msg.key(), err)


def next_reading(bus_id, state):
    state["lat"] += random.uniform(-0.0015, 0.0015)
    state["lon"] += random.uniform(-0.0015, 0.0015)
    state["speed_kmh"] = max(0, min(60, state["speed_kmh"] + random.uniform(-5, 5)))
    return {
        "bus_id": bus_id,
        "route_id": state["route_id"],
        "lat": round(state["lat"], 6),
        "lon": round(state["lon"], 6),
        "speed_kmh": round(state["speed_kmh"], 1),
        "occupancy_pct": random.randint(10, 100),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(target_events_per_sec: int = 2400, duration_sec: int = None):
    bus_ids = list(bus_state.keys())
    sent = 0
    start = time.time()
    tick_batch = max(1, target_events_per_sec // 100)  # send in small ticks

    try:
        while duration_sec is None or (time.time() - start) < duration_sec:
            tick_start = time.time()
            sample = random.sample(bus_ids, min(tick_batch, len(bus_ids)))
            for bus_id in sample:
                state = bus_state[bus_id]
                event = next_reading(bus_id, state)
                producer.produce(
                    TOPIC_BUS_GPS,
                    key=event["route_id"].encode("utf-8"),   # <-- ordering key
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
                sent += 1
            producer.poll(0)
            elapsed = time.time() - tick_start
            sleep_for = max(0.0, 0.01 - elapsed)  # ~100 ticks/sec cadence
            time.sleep(sleep_for)
            if sent % 5000 < tick_batch:
                log.info("Produced %d bus_gps events so far", sent)
    except KeyboardInterrupt:
        log.info("Stopping producer...")
    finally:
        producer.flush(10)
        log.info("Final flush complete. Total sent: %d", sent)


if __name__ == "__main__":
    run()
