#!/usr/bin/env bash
# UrbanPulse - topic creation with per-topic partition counts and retention
# policies, each justified against the data's operational/regulatory lifecycle.
#
# Usage: ./create_topics.sh   (run after `docker compose up -d` in ../docker)

set -euo pipefail
BROKER="kafka1:29092,kafka2:29092,kafka3:29092"
KAFKA_TOPICS="docker exec -i kafka1 kafka-topics --bootstrap-server ${BROKER}"

create() {
  local name=$1 partitions=$2 retention_ms=$3 reason=$4
  echo ">>> Creating ${name} (partitions=${partitions}, retention.ms=${retention_ms})"
  echo "    Justification: ${reason}"
  ${KAFKA_TOPICS} --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms="${retention_ms}" \
    --config cleanup.policy=delete
}

# --- urbanpulse.bus_gps -----------------------------------------------------
# Rate: ~2,400 events/sec, keyed by route_id for per-route ordering.
# 12 partitions: (a) keeps per-partition load to ~200 ev/s, comfortably below
# single-partition throughput ceilings; (b) MetroConnect runs far more than
# 12 routes, so route_id hashing still spreads load evenly across partitions;
# (c) matches the max useful parallelism for a modest 3-6 node Flink/Spark
# consumer pool without over-partitioning (which would raise controller and
# open-file-handle overhead for comparatively little extra throughput).
# Retention: 24 hours ONLY - GPS pings are needed for near-real-time ETA and
# for replaying the last day's movement during accident investigation; there
# is no regulatory reason to retain them longer, and 24h keeps broker disk
# footprint low for a ~2,400 ev/s firehose.
create urbanpulse.bus_gps 12 86400000 \
  "24h retention covers ETA + same-day accident-replay needs only; 12 partitions balance ~200 ev/s/partition against route_id keying across MetroConnect's full route set."

# --- urbanpulse.traffic_signals ---------------------------------------------
# Rate: ~380 events/sec from 3,800 junctions. 6 partitions gives the
# STANDARD_PRIORITY analytics group (3 consumers) two partitions each for
# balanced load, while still letting the HIGH_PRIORITY group's single
# consumer subscribe to all 6 without partition-count becoming a bottleneck.
# Retention: 7 days - long enough to debug a week of signal-timing incidents
# and to feed the Spark ward-aggregation backfill window, without keeping
# high-frequency signal telemetry indefinitely.
create urbanpulse.traffic_signals 6 604800000 \
  "7-day retention supports weekly incident debugging and aggregation backfill; 6 partitions divide evenly across the 3-consumer STANDARD_PRIORITY group."

# --- urbanpulse.air_quality --------------------------------------------------
# Rate: ~60 events/sec from 600 sensors - the lowest-volume stream.
# 3 partitions is deliberately modest: over-partitioning a low-throughput
# topic wastes controller metadata and produces mostly-idle partitions;
# 3 is still enough to parallelise the Flink AQI-emergency job across
# multiple task slots and to survive a single consumer-instance failure
# without starving other partitions.
# Retention: 90 days - explicitly required for pollution TREND ANALYSIS
# (regulatory/public-health reporting looks at rolling quarterly trends).
create urbanpulse.air_quality 3 7776000000 \
  "90-day retention is mandated for quarterly pollution-trend analysis; 3 partitions avoid over-partitioning a comparatively low 60 ev/s stream while keeping Flink parallelism available."

# --- urbanpulse.smart_meters -------------------------------------------------
# Rate: ~1,100 events/sec from 1.1M meters, retained for a full year for
# regulatory energy audits - by far the largest total volume on the cluster.
# 10 partitions gives the Spark Structured Streaming ward-aggregation job
# enough parallel input splits, and spreads the year-long retention's disk
# footprint across more log segments/brokers than a low partition count would.
create urbanpulse.smart_meters 10 31536000000 \
  "365-day retention is mandated for regulatory energy audits; 10 partitions spread the resulting large retained volume across brokers and give Spark enough parallel splits."

# --- urbanpulse.dlq -----------------------------------------------------------
# Dead-letter topic for records that fail validation across all 4 sources.
# Expected volume is a small fraction of total traffic, so 3 partitions is
# sufficient; 14-day retention gives engineers a two-week debugging window
# for data-quality incidents before DLQ records are dropped.
create urbanpulse.dlq 3 1209600000 \
  "Low-volume error stream; 14-day retention gives a two-week window to investigate and fix upstream data-quality issues."

# --- urbanpulse.route_schedule (compacted reference topic for Kafka Streams) -
echo ">>> Creating urbanpulse.route_schedule (compacted GlobalKTable source)"
${KAFKA_TOPICS} --create --if-not-exists \
  --topic urbanpulse.route_schedule \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=compact

# --- urbanpulse.incidents (Flink output) ------------------------------------
echo ">>> Creating urbanpulse.incidents"
${KAFKA_TOPICS} --create --if-not-exists \
  --topic urbanpulse.incidents \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=1209600000

echo ""
echo ">>> Final topic list:"
${KAFKA_TOPICS} --list
