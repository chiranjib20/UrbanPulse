package com.urbanpulse.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Minimal generic JSON Serde backed by Jackson. UrbanPulse's ingest payloads
 * are already JSON, so we (de)serialize straight into a Map rather than
 * hand-rolling per-record classes, keeping the topology focused on the join
 * logic the assignment asks for.
 */
public class JsonSerde<T> implements Serde<T> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> targetType;

    public JsonSerde(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, data) -> {
            try {
                return data == null ? null : mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing JSON for topic " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            try {
                return bytes == null ? null : mapper.readValue(bytes, targetType);
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing JSON for topic " + topic, e);
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static Serde<Map<String, Object>> mapSerde() {
        return (Serde<Map<String, Object>>) (Serde<?>) new JsonSerde<>(Map.class);
    }
}
