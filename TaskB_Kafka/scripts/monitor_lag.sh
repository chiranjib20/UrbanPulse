#!/usr/bin/env bash
# UrbanPulse - side-by-side consumer lag comparison.
# Demonstrates that HIGH_PRIORITY (traffic-signals-high-priority) stays near
# zero lag while STANDARD_PRIORITY (traffic-signals-standard-priority) lag
# grows during a simulated slowdown (see standard_priority_consumer.py).
#
# Usage: watch -n 2 ./monitor_lag.sh   (or just run repeatedly)

set -euo pipefail
BROKER="kafka1:29092,kafka2:29092,kafka3:29092"

echo "=================== HIGH_PRIORITY (traffic-signals-high-priority) ==================="
docker exec -i kafka1 kafka-consumer-groups \
  --bootstrap-server "${BROKER}" \
  --describe --group traffic-signals-high-priority || echo "(group not active yet)"

echo ""
echo "================= STANDARD_PRIORITY (traffic-signals-standard-priority) ============="
docker exec -i kafka1 kafka-consumer-groups \
  --bootstrap-server "${BROKER}" \
  --describe --group traffic-signals-standard-priority || echo "(group not active yet)"
