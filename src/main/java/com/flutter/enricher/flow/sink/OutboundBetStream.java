package com.flutter.enricher.flow.sink;

import com.flutter.enricher.model.bet.OutboundFootballBet;
import com.flutter.enricher.serde.KafkaPojoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Creates a kafka sink for the outbound bets.
 */
public class OutboundBetStream {

    private static final String KAFKA_OUTBOUND_BOOTSTRAP_SERVERS_PROPERTY = "kafka.outbound.bootstrap.servers";
    private static final String KAFKA_OUTBOUND_BET_TOPIC_PROPERTY = "kafka.outbound.bet.topic";

    /**
     * Creates the a kafka producer to publish the outbound bets.
     *
     * @return A kafka producer managed by flink.
     */
    public static FlinkKafkaProducer<OutboundFootballBet> get(ParameterTool params) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.get(KAFKA_OUTBOUND_BOOTSTRAP_SERVERS_PROPERTY));

        return new FlinkKafkaProducer<>(
                params.get(KAFKA_OUTBOUND_BET_TOPIC_PROPERTY),
                new KafkaPojoSerializer<>(),
                properties);
    }
}
