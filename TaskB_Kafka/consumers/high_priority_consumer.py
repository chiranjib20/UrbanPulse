"""
UrbanPulse - HIGH_PRIORITY Consumer (Traffic Signal Control)
=============================================================
One consumer, one consumer group ("traffic-signals-high-priority"), reading
ALL partitions of urbanpulse.traffic_signals. Because it is the ONLY member
of its group, Kafka's group coordinator assigns it every partition, so it
sees every signal event with minimal fan-out overhead and near-zero
processing time per message - simulating the real-time adaptive signal
control system, which cannot tolerate queueing behind analytics workloads.

Run exactly one instance of this script. Compare its consumer lag against
STANDARD_PRIORITY (3 consumers, deliberately slower) using monitor_lag.sh -
HIGH_PRIORITY lag should stay near zero even while STANDARD_PRIORITY lag
grows during a simulated slowdown.
"""
import json
import logging
import time

from confluent_kafka import Consumer

from common_config import BOOTSTRAP_SERVERS, TOPIC_TRAFFIC_SIGNALS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("high_priority_consumer")

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "traffic-signals-high-priority",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 1000,
    # Tight session/heartbeat so a stalled real-time consumer is detected fast.
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 15000,
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC_TRAFFIC_SIGNALS])

WAIT_THRESHOLD_SEC = 180
GRIDLOCK_CYCLES_REQUIRED = 3
_recent_waits = {}  # junction_id -> list of recent avg_wait_sec


def process_signal_event(event: dict):
    """Near-zero-latency processing: cheap threshold check only, matching the
    real-time signal control system's need for sub-second reaction time."""
    junction = event["junction_id"]
    waits = _recent_waits.setdefault(junction, [])
    waits.append(event["avg_wait_sec"])
    if len(waits) > GRIDLOCK_CYCLES_REQUIRED:
        waits.pop(0)

    if len(waits) == GRIDLOCK_CYCLES_REQUIRED and all(w > WAIT_THRESHOLD_SEC for w in waits):
        log.warning(
            "GRIDLOCK signal at junction=%s zone=%s (avg_wait=%.0fs over %d cycles) -> adapting signal phase",
            junction, event["zone"], sum(waits) / len(waits), GRIDLOCK_CYCLES_REQUIRED,
        )
        # In production this would call the signal-control actuation API.


def run():
    log.info("HIGH_PRIORITY consumer started - reading ALL partitions solo.")
    try:
        while True:
            msg = consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue
            event = json.loads(msg.value())
            process_signal_event(event)
    except KeyboardInterrupt:
        log.info("Shutting down HIGH_PRIORITY consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
