package com.flutter.enricher.serde;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Generic Kafka POJO Serializer to be used in kafka sinks.
 */
public class KafkaPojoSerializer<T> implements SerializationSchema<T> {

    @Override
    public byte[] serialize(T element) {
        try {
            return ObjectMapperSingleton.getInstance().writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed when serializing an inbound bet.");
        }
    }

    private static class ObjectMapperSingleton {
        static ObjectMapper getInstance() {
            return new ObjectMapper();
        }
    }
}

