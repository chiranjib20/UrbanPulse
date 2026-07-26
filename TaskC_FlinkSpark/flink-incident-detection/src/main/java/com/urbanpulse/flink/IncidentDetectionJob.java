package com.urbanpulse.flink;

import com.urbanpulse.flink.functions.AqiEmergencyFunction;
import com.urbanpulse.flink.functions.BusBunchingDetector;
import com.urbanpulse.flink.functions.GridlockDetector;
import com.urbanpulse.flink.model.AirQualityEvent;
import com.urbanpulse.flink.model.BusGpsEvent;
import com.urbanpulse.flink.model.IncidentAlert;
import com.urbanpulse.flink.model.TrafficSignalEvent;
import com.urbanpulse.flink.serde.JsonSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * UrbanPulse - Task C, Problem 9: Flink Real-Time Incident Detection.
 *
 * Reads urbanpulse.air_quality, urbanpulse.traffic_signals and
 * urbanpulse.bus_gps, applies event-time watermarks (tolerating a bounded
 * amount of out-of-orderness typical of city sensor networks), keys each
 * stream by the entity the incident rule cares about, and routes all three
 * detectors' output into a single urbanpulse.incidents topic.
 */
public class IncidentDetectionJob {

    private static final String BOOTSTRAP =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9095");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000); // 30s checkpoints - fast enough recovery for a 2-min alert SLA

        // ---- Air Quality -> AQI Emergency -------------------------------
        KafkaSource<AirQualityEvent> aqiSource = KafkaSource.<AirQualityEvent>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("urbanpulse.air_quality")
                .setGroupId("flink-incident-detection")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonSchema<>(AirQualityEvent.class))
                .build();

        DataStream<AirQualityEvent> aqiStream = env.fromSource(
                aqiSource,
                WatermarkStrategy.<AirQualityEvent>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                        .withTimestampAssigner((event, ts) -> event.eventTimeMillis()),
                "air-quality-source"
        );

        DataStream<IncidentAlert> aqiAlerts = aqiStream
                .keyBy(e -> e.sensor_id)
                .process(new AqiEmergencyFunction())
                .name("aqi-emergency-detector");

        // ---- Traffic Signals -> Gridlock ---------------------------------
        KafkaSource<TrafficSignalEvent> signalSource = KafkaSource.<TrafficSignalEvent>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("urbanpulse.traffic_signals")
                .setGroupId("flink-incident-detection")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonSchema<>(TrafficSignalEvent.class))
                .build();

        DataStream<TrafficSignalEvent> signalStream = env.fromSource(
                signalSource,
                WatermarkStrategy.<TrafficSignalEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.eventTimeMillis()),
                "traffic-signals-source"
        );

        DataStream<IncidentAlert> gridlockAlerts = signalStream
                .keyBy(e -> e.junction_id)
                .process(new GridlockDetector())
                .name("gridlock-detector");

        // ---- Bus GPS -> Bunching ------------------------------------------
        KafkaSource<BusGpsEvent> gpsSource = KafkaSource.<BusGpsEvent>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setTopics("urbanpulse.bus_gps")
                .setGroupId("flink-incident-detection")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonSchema<>(BusGpsEvent.class))
                .build();

        DataStream<BusGpsEvent> gpsStream = env.fromSource(
                gpsSource,
                WatermarkStrategy.<BusGpsEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.eventTimeMillis()),
                "bus-gps-source"
        );

        DataStream<IncidentAlert> bunchingAlerts = gpsStream
                .keyBy(e -> e.route_id)
                .process(new BusBunchingDetector())
                .name("bus-bunching-detector");

        // ---- Union all incident types into one sink ------------------------
        DataStream<IncidentAlert> allIncidents = aqiAlerts.union(gridlockAlerts, bunchingAlerts);

        KafkaSink<IncidentAlert> incidentSink = KafkaSink.<IncidentAlert>builder()
                .setBootstrapServers(BOOTSTRAP)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("urbanpulse.incidents")
                                .setValueSerializationSchema(new JsonSchema<>(IncidentAlert.class))
                                .build())
                .build();

        allIncidents.sinkTo(incidentSink).name("incidents-sink");

        env.execute("UrbanPulse Incident Detection");
    }
}
