package com.urbanpulse.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class JsonSchema<T> implements DeserializationSchema<T>, SerializationSchema<T> {

    private final Class<T> type;
    private transient ObjectMapper mapper;

    public JsonSchema(Class<T> type) {
        this.type = type;
    }

    private ObjectMapper mapper() {
        if (mapper == null) mapper = new ObjectMapper();
        return mapper;
    }

    @Override
    public T deserialize(byte[] message) throws java.io.IOException {
        if (message == null) return null;
        return mapper().readValue(message, type);
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper().writeValueAsBytes(element);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }
}
