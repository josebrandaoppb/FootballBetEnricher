package com.flutter.enricher.flow.source;

import com.flutter.enricher.model.bet.InboundFootballBet;
import com.flutter.enricher.serde.KafkaPojoDeserializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Create a kafka consumer for the inbound football bets.
 */
public class InboundBetStream {

    private static final String KAFKA_INBOUND_BOOTSTRAP_SERVERS_PROPERTY = "kafka.inbound.bootstrap.servers";
    private static final String KAFKA_INBOUND_GROUP_ID_PROPERTY = "kafka.inbound.group.id";
    private static final String KAFKA_INBOUND_BET_OFFSET_RESET_STRATEGY_PROPERTY = "kafka.inbound.bet.auto.offset.reset";
    private static final String KAFKA_INBOUND_BET_TOPIC_PATTERN_PROPERTY = "kafka.inbound.bet.topic";

    /**
     * Creates the a kafka consumer to consume inbound bets.
     *
     * @param params Configuration parameters to apply on the consumer configuration.
     * @return Data stream source of inbound bets.
     */
    public static FlinkKafkaConsumer<InboundFootballBet> get(ParameterTool params) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.get(KAFKA_INBOUND_BOOTSTRAP_SERVERS_PROPERTY));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, params.get(KAFKA_INBOUND_GROUP_ID_PROPERTY));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, params.get(KAFKA_INBOUND_BET_OFFSET_RESET_STRATEGY_PROPERTY));

        return new FlinkKafkaConsumer<>(
                Pattern.compile(params.get(KAFKA_INBOUND_BET_TOPIC_PATTERN_PROPERTY)),
                new KafkaPojoDeserializer<>(InboundFootballBet.class),
                properties);

    }
}
