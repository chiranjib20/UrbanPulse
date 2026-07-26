# UrbanPulse — Build Guide (Team Walkthrough)

This README is the step-by-step story of how UrbanPulse was built, in the
order to build/present it. Each step says **what** we built, **why**, and
**where** the file lives, so anyone on the team can follow along or rebuild
a piece from scratch.

Full technical reports (with diagrams and grading-rubric-aligned writeups)
are in `TaskA_Architecture/`, `TaskB_Kafka/`, `TaskC_FlinkSpark/` as `.docx`
files — read those for the graded deliverable content. This file is the
"how we got here and how to run it" narrative for the team.

---

## Step 0 — Understand the constraint that shapes everything

MetroConnect needs BOTH (a) sub-2-minute incident alerts and 90-second
signal adaptation, AND (b) weekly/monthly batch reports for councillors and
the state government. That dual requirement is why **every** later decision
(Lambda over Kappa, Flink for alerts, Spark for aggregation) looks the way
it does — it's not a default stack choice, it's forced by having two very
different consumers of the same data.

## Step 1 — Design the architecture first (Task A)

1. Draw the four sources → Kafka → split processing (speed + batch) →
   storage → serving layers. See `TaskA_Architecture/architecture.png`.
2. Pick storage tech **per workload**, not one database for everything:
   TimescaleDB (time-series), PostGIS (geospatial), MinIO+Parquet (archive),
   ClickHouse (OLAP rollups for councillor reports).
3. Build the Lambda vs Kappa matrix — the deciding row is *"Compliance with
   Government Reporting Mandate"*: Lambda's batch layer gives an auditable,
   reproducible artifact for state submissions; Kappa would need to treat
   one stream replay as "the" canonical report, which auditors dislike.
4. Write the government readiness checklist (data sovereignty, open-source,
   DR, ward-officer UX) — this becomes your non-functional requirements list
   for Tasks B and C.

**Deliverable:** `TaskA_Architecture/UrbanPulse_TaskA_Architecture.docx`

## Step 2 — Stand up the Kafka backbone (Task B)

1. **Cluster**: `TaskB_Kafka/docker/docker-compose.yml` — 3 brokers, KRaft
   mode, replication factor 3, min.insync.replicas 2 everywhere.
   ```
   cd TaskB_Kafka/docker && docker compose up -d
   ```
2. **Topics**: `TaskB_Kafka/scripts/create_topics.sh` — run it, then read the
   comments inline; every partition count and retention value is justified
   against either throughput or a regulatory retention requirement (90 days
   AQI, 365 days meters), not picked arbitrarily.
3. **Producers**: start with `bus_gps_producer.py` — explain the `key=route_id`
   line first, since that's the ordering guarantee the whole enrichment step
   later depends on. Then `air_quality_producer.py` — walk through
   `send_with_retry()` to show at-least-once semantics concretely, and the
   5% null-AQI simulation that feeds the DLQ story.
4. **Priority consumers**: run `high_priority_consumer.py` (1 instance) and
   three copies of `standard_priority_consumer.py`, one with
   `SIMULATE_SLOWDOWN=1`. Then run `scripts/monitor_lag.sh` side-by-side and
   show the team the LAG column diverging — this is the most convincing live
   demo in the whole project.
5. **Enrichment**: `scripts/load_route_schedule.py` loads the CSV into a
   compacted topic; `streams/EnrichmentTopology.java` joins it against
   `bus_gps` via a GlobalKTable. Explain *why* GlobalKTable (avoids
   repartitioning the high-volume GPS stream just to match a tiny reference
   table).
6. **DLQ**: `scripts/dlq_router.py` — show the validation rules, then let it
   run for a few minutes and read out the error-distribution report it logs.

**Deliverable:** `TaskB_Kafka/UrbanPulse_TaskB_Kafka.docx` + all code above.

## Step 3 — Add the two processing engines (Task C)

1. **Flink (speed layer)** — `TaskC_FlinkSpark/flink-incident-detection/`.
   Walk the team through the three detectors in increasing complexity:
   - `AqiEmergencyFunction` (stateless threshold + alert-suppression state)
   - `GridlockDetector` (consecutive-breach counter, resets on a miss)
   - `BusBunchingDetector` (the hardest one — pairwise state per route,
     haversine distance, continuous-proximity streak). Draw the state
     machine on a whiteboard before showing code: CLOSE → (5 min) → ALERTED
     → (separates) → reset.
   ```
   cd flink-incident-detection && mvn clean package -q
   flink run -c com.urbanpulse.flink.IncidentDetectionJob target/flink-incident-detection.jar
   ```
2. **Spark (near-batch layer)** — `TaskC_FlinkSpark/spark-ward-analytics/`.
   - `ward_energy_aggregation.py`: 15-min tumbling window, 45-min watermark,
     `foreachBatch` fanning out to Kafka **and** partitioned Parquet in the
     same micro-batch — point out this is exactly the dual-sink pattern the
     Lambda architecture's batch layer needs.
   - `aqi_health_advisory.py`: rolling (sliding) window expressed as literal
     Spark SQL, broadcast-joined with `zone_profile.csv`, Update output mode.
   ```
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ward_energy_aggregation.py
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 aqi_health_advisory.py
   ```
3. **Comparison writeup** — present the 4-dimension table (state size,
   latency, RTO, operational complexity) and let the team see *why* bus
   bunching went to Flink and ward aggregation went to Spark, rather than
   presenting it as "we just picked one."

**Deliverable:** `TaskC_FlinkSpark/UrbanPulse_TaskC_FlinkSpark.docx` + code.

## Step 4 — Package for submission

```
UrbanPulse/
├── README.md                          <- this file
├── TaskA_Architecture/
│   ├── UrbanPulse_TaskA_Architecture.docx
│   └── architecture.png
├── TaskB_Kafka/
│   ├── UrbanPulse_TaskB_Kafka.docx
│   ├── docker/docker-compose.yml
│   ├── scripts/ (create_topics.sh, load_route_schedule.py, dlq_router.py, monitor_lag.sh)
│   ├── producers/ (bus_gps_producer.py, air_quality_producer.py, common_config.py)
│   ├── consumers/ (high_priority_consumer.py, standard_priority_consumer.py)
│   └── streams/ (pom.xml, route_schedule.csv, src/.../EnrichmentTopology.java)
└── TaskC_FlinkSpark/
    ├── UrbanPulse_TaskC_FlinkSpark.docx
    ├── flink-incident-detection/ (pom.xml, src/.../*.java)
    └── spark-ward-analytics/ (ward_energy_aggregation.py, aqi_health_advisory.py, zone_profile.csv)
```

Push this whole tree to one Git repo (as the submission requires), then zip
the repo + a screen recording walking through Steps 2–3 live, per the
Submission Requirements in the assignment brief.

## Talking points if asked "why not just use one engine everywhere?"

- A single Kappa+Flink-only design would need to replay a full year of
  Kafka log to satisfy the smart-meter regulatory audit — not cost-effective
  and less auditable than a scheduled Spark batch run (Task A, Section 2).
- A single Spark-only design can't hit the sub-2-minute AQI / 90-second
  signal SLA at true event granularity — Spark's unit of work is a
  micro-batch, not a single event.
- Running both is more operational surface, which is exactly why Task A's
  checklist item on DR drills and shared business-logic libraries exists —
  the cost of Lambda is real and has to be actively managed, not ignored.
