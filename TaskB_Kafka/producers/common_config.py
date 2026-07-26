"""Shared configuration for UrbanPulse Kafka producers and consumers."""

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"

TOPIC_BUS_GPS = "urbanpulse.bus_gps"
TOPIC_TRAFFIC_SIGNALS = "urbanpulse.traffic_signals"
TOPIC_AIR_QUALITY = "urbanpulse.air_quality"
TOPIC_SMART_METERS = "urbanpulse.smart_meters"
TOPIC_DLQ = "urbanpulse.dlq"
TOPIC_ROUTE_SCHEDULE = "urbanpulse.route_schedule"
TOPIC_INCIDENTS = "urbanpulse.incidents"

ROUTES = [f"RT-{i:03d}" for i in range(1, 121)]     # 120 simulated bus routes
BUSES_PER_ROUTE = 100                                # -> 12,000 buses total
JUNCTIONS = [f"JN-{i:04d}" for i in range(1, 3801)]  # 3,800 junctions
AQI_SENSORS = [f"AQS-{i:03d}" for i in range(1, 601)]  # 600 sensors
ZONES = [f"ZONE-{i:02d}" for i in range(1, 26)]      # 25 city zones
WARDS = [f"WARD-{i:03d}" for i in range(1, 151)]     # 150 wards
