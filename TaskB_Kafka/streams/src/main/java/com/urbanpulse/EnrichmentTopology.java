package com.urbanpulse;

import com.urbanpulse.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * UrbanPulse - Real-time GPS x Route-Schedule Enrichment (Task B, Problem 7)
 * ============================================================================
 * Joins the live urbanpulse.bus_gps KStream (keyed by route_id) against the
 * urbanpulse.route_schedule reference data, loaded as a GlobalKTable so every
 * stream task has the full route table locally - avoiding a repartition of
 * the (much higher-volume) GPS stream just to co-partition it with reference
 * data. Output: urbanpulse.bus_gps_enriched, carrying scheduled_arrival_time,
 * route_name and terminal alongside the original GPS position - this is the
 * foundation for the real-time bus ETA service.
 */
public class EnrichmentTopology {

    private static final String BUS_GPS_TOPIC = "urbanpulse.bus_gps";
    private static final String ROUTE_SCHEDULE_TOPIC = "urbanpulse.route_schedule";
    private static final String ENRICHED_OUTPUT_TOPIC = "urbanpulse.bus_gps_enriched";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "urbanpulse-route-enrichment");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9095"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        // At-least-once is sufficient for an enrichment feed to a best-effort
        // ETA service; exactly_once_v2 could be enabled here if the enriched
        // stream fed a billing/financial system instead.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Map<String, Object>> jsonSerde = JsonSerde.mapSerde();

        // Reference data: GlobalKTable gives every task a full local copy,
        // so the join below needs no repartitioning of the GPS stream.
        GlobalKTable<String, Map<String, Object>> routeSchedule = builder.globalTable(
                ROUTE_SCHEDULE_TOPIC,
                Consumed.with(Serdes.String(), jsonSerde)
        );

        KStream<String, Map<String, Object>> busGps = builder.stream(
                BUS_GPS_TOPIC,
                Consumed.with(Serdes.String(), jsonSerde)
        );

        KStream<String, Map<String, Object>> enriched = busGps.leftJoin(
                routeSchedule,
                (gpsKey, gpsValue) -> (String) gpsValue.get("route_id"), // key extractor into the GlobalKTable
                (gpsValue, scheduleValue) -> {
                    Map<String, Object> out = new HashMap<>(gpsValue);
                    if (scheduleValue != null) {
                        out.put("scheduled_arrival_time", scheduleValue.get("scheduled_arrival_time"));
                        out.put("route_name", scheduleValue.get("route_name"));
                        out.put("terminal", scheduleValue.get("terminal"));
                        out.put("schedule_match", true);
                    } else {
                        // Route not yet in the reference table - still forward the
                        // GPS position (leftJoin), flagged so the ETA service can
                        // fall back to a default estimate instead of dropping data.
                        out.put("schedule_match", false);
                    }
                    return out;
                }
        );

        enriched.to(ENRICHED_OUTPUT_TOPIC, Produced.with(Serdes.String(), jsonSerde));

        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        streams.setUncaughtExceptionHandler(ex -> {
            System.err.println("Uncaught Streams exception: " + ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
