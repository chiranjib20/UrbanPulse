"""
UrbanPulse - Dead-Letter Queue (DLQ) Validation Router
========================================================
Consumes all four ingest topics, applies per-topic validation rules, and
routes any record that fails validation to urbanpulse.dlq with an
`error_reason` field describing why, plus the original topic/payload for
debugging. Valid records are simply acknowledged (in a real deployment they
would be handed to the next processing stage - Flink/Spark - which is out
of scope for this router).

Validation rules (Problem 8):
  - null values          -> any required numeric field is None
  - out-of-range AQI      -> aqi present but outside [0, 500]
  - impossible GPS coords -> lat/lon outside MetroConnect's plausible bbox
"""
import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from common_config import (
    BOOTSTRAP_SERVERS, TOPIC_BUS_GPS, TOPIC_TRAFFIC_SIGNALS,
    TOPIC_AIR_QUALITY, TOPIC_SMART_METERS, TOPIC_DLQ,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dlq_router")

# Loosely bounds MetroConnect's plausible metro-area coordinates.
LAT_BOUNDS = (12.0, 14.0)
LON_BOUNDS = (76.5, 78.5)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "urbanpulse-dlq-validator",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
})
consumer.subscribe([TOPIC_BUS_GPS, TOPIC_TRAFFIC_SIGNALS, TOPIC_AIR_QUALITY, TOPIC_SMART_METERS])

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})

error_counts = Counter()


def validate_bus_gps(e: dict):
    lat, lon = e.get("lat"), e.get("lon")
    if lat is None or lon is None:
        return "NULL_GPS_COORDINATE"
    if not (LAT_BOUNDS[0] <= lat <= LAT_BOUNDS[1]) or not (LON_BOUNDS[0] <= lon <= LON_BOUNDS[1]):
        return "IMPOSSIBLE_GPS_COORDINATE"
    if e.get("speed_kmh") is None or e["speed_kmh"] < 0 or e["speed_kmh"] > 120:
        return "INVALID_SPEED"
    return None


def validate_traffic_signals(e: dict):
    if e.get("avg_wait_sec") is None or e.get("vehicle_count") is None:
        return "NULL_SIGNAL_METRIC"
    if e["avg_wait_sec"] < 0:
        return "NEGATIVE_WAIT_TIME"
    return None


def validate_air_quality(e: dict):
    if e.get("aqi") is None:
        return "NULL_AQI_VALUE"
    if not (0 <= e["aqi"] <= 500):
        return "OUT_OF_RANGE_AQI"
    return None


def validate_smart_meters(e: dict):
    if e.get("kwh_reading") is None or e.get("voltage") is None:
        return "NULL_METER_READING"
    if e.get("voltage") is not None and not (150 <= e["voltage"] <= 280):
        return "OUT_OF_RANGE_VOLTAGE"
    return None


VALIDATORS = {
    TOPIC_BUS_GPS: validate_bus_gps,
    TOPIC_TRAFFIC_SIGNALS: validate_traffic_signals,
    TOPIC_AIR_QUALITY: validate_air_quality,
    TOPIC_SMART_METERS: validate_smart_meters,
}


def send_to_dlq(source_topic: str, key, raw_value: bytes, error_reason: str):
    dlq_record = {
        "source_topic": source_topic,
        "error_reason": error_reason,
        "original_key": key.decode("utf-8") if key else None,
        "original_payload": json.loads(raw_value),
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
    }
    producer.produce(TOPIC_DLQ, key=key, value=json.dumps(dlq_record).encode("utf-8"))
    error_counts[error_reason] += 1


def run(report_every_sec: int = 300):
    log.info("DLQ validator started, subscribed to 4 source topics.")
    window_start = time.time()
    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                if time.time() - window_start >= report_every_sec:
                    emit_report(report_every_sec)
                    window_start = time.time()
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            topic = msg.topic()
            try:
                event = json.loads(msg.value())
            except json.JSONDecodeError:
                send_to_dlq(topic, msg.key(), msg.value(), "MALFORMED_JSON")
                continue

            reason = VALIDATORS[topic](event)
            if reason:
                send_to_dlq(topic, msg.key(), msg.value(), reason)

            if time.time() - window_start >= report_every_sec:
                producer.flush(5)
                emit_report(report_every_sec)
                window_start = time.time()
    except KeyboardInterrupt:
        log.info("Shutting down DLQ validator...")
    finally:
        producer.flush(10)
        consumer.close()


def emit_report(window_sec: int):
    total = sum(error_counts.values())
    log.info("=== DLQ report (last ~%ds window): %d records quarantined ===", window_sec, total)
    for reason, count in error_counts.most_common():
        pct = 100.0 * count / total if total else 0.0
        log.info("  %-28s %6d  (%.1f%%)", reason, count, pct)


if __name__ == "__main__":
    run()
