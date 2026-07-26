"""
UrbanPulse - Air Quality Producer
=================================
Simulates ~60 events/sec from 600 air-quality sensors.

Delivery semantics: AT-LEAST-ONCE (explicit).
  - Idempotence is intentionally left OFF and we manage retries ourselves,
    so a message may be re-sent (and therefore possibly duplicated
    downstream) if the broker doesn't acknowledge in time - the classic
    at-least-once trade-off. Downstream (Kafka Streams / Spark) is expected
    to be tolerant of duplicate AQI readings (e.g. idempotent upserts keyed
    on sensor_id+timestamp), which is a reasonable assumption for a
    monitoring signal that already tolerates minor duplication.
  - acks='all' ensures the message is durable on all in-sync replicas
    before an ack is returned; our own retry loop resends on NO ack /
    on error, up to MAX_RETRIES, with exponential backoff.

Simulated sensor failure: ~5% of readings arrive with a null AQI value
(sensor timeout). These are NOT dropped silently - they are logged as a
warning and still published, tagged with sensor_status=TIMEOUT, so the
downstream DLQ validation stage (urbanpulse.dlq) can catch and quantify
them per Task B, Problem 8.
"""
import json
import logging
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from common_config import BOOTSTRAP_SERVERS, TOPIC_AIR_QUALITY, AQI_SENSORS, ZONES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("air_quality_producer")

MAX_RETRIES = 5
NULL_AQI_RATE = 0.05  # 5% simulated sensor timeout

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "enable.idempotence": False,   # explicit at-least-once, duplicates possible
    "retries": 0,                  # we handle retries ourselves (see send_with_retry)
    "linger.ms": 20,
}

producer = Producer(producer_conf)

sensor_zone = {s: random.choice(ZONES) for s in AQI_SENSORS}


def make_reading(sensor_id: str) -> dict:
    is_timeout = random.random() < NULL_AQI_RATE
    pm25 = None if is_timeout else round(random.uniform(20, 400), 1)
    pm10 = None if is_timeout else round(random.uniform(30, 500), 1)
    no2 = None if is_timeout else round(random.uniform(5, 120), 1)
    aqi = None if is_timeout else round(random.uniform(20, 450), 0)
    return {
        "sensor_id": sensor_id,
        "zone": sensor_zone[sensor_id],
        "pm25": pm25,
        "pm10": pm10,
        "no2": no2,
        "aqi": aqi,
        "sensor_status": "TIMEOUT" if is_timeout else "OK",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def send_with_retry(topic: str, key: str, value: dict) -> bool:
    """Send with manual at-least-once retry + exponential backoff.
    Returns True once the broker has acknowledged the write."""
    payload = json.dumps(value).encode("utf-8")
    delivered = {"ok": False, "error": None}

    def _cb(err, msg):
        if err is not None:
            delivered["error"] = err
        else:
            delivered["ok"] = True

    for attempt in range(1, MAX_RETRIES + 1):
        delivered["ok"] = False
        delivered["error"] = None
        try:
            producer.produce(topic, key=key.encode("utf-8"), value=payload, callback=_cb)
        except BufferError:
            log.warning("Local queue full, waiting before retry %d", attempt)
            producer.poll(1.0)
            continue

        producer.flush(5)  # block until delivery callback fires or times out
        if delivered["ok"]:
            return True

        backoff = min(2 ** attempt * 0.1, 3.0)
        log.warning(
            "Delivery attempt %d/%d failed for sensor=%s (%s); retrying in %.2fs",
            attempt, MAX_RETRIES, key, delivered["error"], backoff,
        )
        time.sleep(backoff)

    log.error("Giving up on sensor=%s after %d attempts - message dropped", key, MAX_RETRIES)
    return False


def run(duration_sec: int = None):
    sent, failed, null_count = 0, 0, 0
    start = time.time()
    try:
        while duration_sec is None or (time.time() - start) < duration_sec:
            tick_start = time.time()
            for _ in range(60):  # ~60 events/sec, one tick per second
                sensor_id = random.choice(AQI_SENSORS)
                reading = make_reading(sensor_id)
                if reading["sensor_status"] == "TIMEOUT":
                    null_count += 1
                    log.warning(
                        "Sensor %s timed out - forwarding null AQI reading for DLQ validation (count=%d)",
                        sensor_id, null_count,
                    )
                ok = send_with_retry(TOPIC_AIR_QUALITY, sensor_id, reading)
                sent += 1 if ok else 0
                failed += 0 if ok else 1
            elapsed = time.time() - tick_start
            time.sleep(max(0.0, 1.0 - elapsed))
            log.info("Progress: sent=%d failed=%d null_aqi=%d", sent, failed, null_count)
    except KeyboardInterrupt:
        log.info("Stopping producer...")
    finally:
        producer.flush(10)
        log.info("Final: sent=%d failed=%d null_aqi_pct=%.1f%%",
                  sent, failed, 100.0 * null_count / max(1, sent + failed))


if __name__ == "__main__":
    run()
