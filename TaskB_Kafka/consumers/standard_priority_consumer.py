"""
UrbanPulse - STANDARD_PRIORITY Consumer (Analytics Dashboard)
===============================================================
Run THREE instances of this script (same group.id -> Kafka spreads the 6
urbanpulse.traffic_signals partitions two-per-consumer). This group feeds
the analytics dashboard, which tolerates seconds-to-minutes of latency, so
each instance takes an instance index argument purely for logging.

To *simulate a processing slowdown* (per Problem 6), set SIMULATE_SLOWDOWN=1
in the environment for one or more instances - each message then sleeps for
SLOWDOWN_MS, causing this group's consumer lag to grow while
HIGH_PRIORITY's lag (a separate group reading the same topic) stays
near zero, because group lag is tracked independently per consumer group.

Usage:
    python standard_priority_consumer.py 1
    python standard_priority_consumer.py 2
    SIMULATE_SLOWDOWN=1 python standard_priority_consumer.py 3
"""
import json
import logging
import os
import sys
import time

from confluent_kafka import Consumer

from common_config import BOOTSTRAP_SERVERS, TOPIC_TRAFFIC_SIGNALS

instance_id = sys.argv[1] if len(sys.argv) > 1 else "0"
logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [instance-{instance_id}] %(message)s")
log = logging.getLogger("standard_priority_consumer")

SIMULATE_SLOWDOWN = os.environ.get("SIMULATE_SLOWDOWN", "0") == "1"
SLOWDOWN_MS = int(os.environ.get("SLOWDOWN_MS", "800"))

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "traffic-signals-standard-priority",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 2000,
    "session.timeout.ms": 30000,
    "max.poll.interval.ms": 300000,  # tolerate slow processing without a rebalance storm
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_TRAFFIC_SIGNALS])

processed = 0


def process_for_dashboard(event: dict):
    """Heavier, non-time-critical aggregation work for the analytics
    dashboard - safe to be slow, unlike the HIGH_PRIORITY path."""
    global processed
    processed += 1
    if SIMULATE_SLOWDOWN:
        time.sleep(SLOWDOWN_MS / 1000.0)
    if processed % 200 == 0:
        log.info("Dashboard aggregation processed %d events (slowdown=%s)", processed, SIMULATE_SLOWDOWN)


def run():
    log.info("STANDARD_PRIORITY consumer started (slowdown=%s).", SIMULATE_SLOWDOWN)
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue
            event = json.loads(msg.value())
            process_for_dashboard(event)
    except KeyboardInterrupt:
        log.info("Shutting down STANDARD_PRIORITY consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
