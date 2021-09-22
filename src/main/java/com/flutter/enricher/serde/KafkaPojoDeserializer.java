package com.flutter.enricher.serde;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * Generic Kafka POJO Deserializer to be used in kafka sources.
 */
public class KafkaPojoDeserializer<T> implements KafkaDeserializationSchema<T> {

    final Class<T> typeParameterClass;

    public KafkaPojoDeserializer(Class<T> typeParameterClass) {
        this.typeParameterClass = typeParameterClass;
    }

    /**
     * Method to decide whether the element signals the end of the stream. If true is returned the element won't be
     * emitted.
     *
     * @return True, if the bet signals end of stream, false otherwise.
     */
    @Override
    public boolean isEndOfStream(T message) {
        return false;
    }

    /**
     * Deserializes the Kafka record.
     *
     * @param record Kafka record to be deserialized.
     * @return The deserialized message as an inbound bet.
     */
    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws IOException {
        return ObjectMapperSingleton.getInstance().readValue(consumerRecord.value(), typeParameterClass);
    }

    /**
     * Gets the inbound bet data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The bet data type.
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return Types.POJO(typeParameterClass);
    }


    private static class ObjectMapperSingleton {
        static ObjectMapper getInstance() {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            return objectMapper;
        }
    }
}

