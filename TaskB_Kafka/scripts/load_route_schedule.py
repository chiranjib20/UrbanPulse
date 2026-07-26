"""
UrbanPulse - Route Schedule Loader
====================================
Publishes each row of route_schedule.csv (provided reference data) into the
compacted urbanpulse.route_schedule topic, keyed by route_id. The Kafka
Streams enrichment topology (EnrichmentTopology.java) consumes this topic
as a GlobalKTable so every stream task has a full local copy of the
route -> schedule mapping to join against, regardless of which partition
a given bus_gps record lands on.

Run this once (or whenever route_schedule.csv changes) before starting the
Kafka Streams application.
"""
import csv
import json
import sys

from confluent_kafka import Producer

from common_config import BOOTSTRAP_SERVERS, TOPIC_ROUTE_SCHEDULE

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS, "acks": "all"})


def load(csv_path: str):
    count = 0
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["route_id"]
            producer.produce(
                TOPIC_ROUTE_SCHEDULE,
                key=key.encode("utf-8"),
                value=json.dumps(row).encode("utf-8"),
            )
            count += 1
    producer.flush(10)
    print(f"Loaded {count} route_schedule rows into {TOPIC_ROUTE_SCHEDULE}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "route_schedule.csv"
    load(path)
