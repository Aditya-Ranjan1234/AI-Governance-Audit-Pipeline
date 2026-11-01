package io.governance.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public final class JsonSerde<T> implements Serde<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final Class<T> type;

    public JsonSerde(Class<T> type) { this.type = type; }

    @Override public Serializer<T> serializer() {
        return (topic, value) -> {
            if (value == null) return null;
            try { return MAPPER.writeValueAsBytes(value); }
            catch (Exception e) { throw new SerializationException("Cannot serialize " + type.getSimpleName(), e); }
        };
    }

    @Override public Deserializer<T> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try { return MAPPER.readValue(bytes, type); }
            catch (Exception e) { throw new SerializationException("Cannot deserialize " + type.getSimpleName(), e); }
        };
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}
}
