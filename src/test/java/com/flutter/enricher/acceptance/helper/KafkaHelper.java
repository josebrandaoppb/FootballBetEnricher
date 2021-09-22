package com.flutter.enricher.acceptance.helper;

import com.flutter.enricher.model.bet.InboundFootballBet;
import com.flutter.enricher.model.bet.OutboundFootballBet;
import com.flutter.enricher.model.participant.ParticipantData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Kafka helper methods.
 * It provides the different kafka producers and consumers we need to test our application.
 */
@Slf4j
public class KafkaHelper {
    private static final int MAX_RETRIES = 10;
    private static final String TEST_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TEST_BET_INBOUND_TOPIC = "inbound-football-bets";
    private static final String TEST_BET_OUTBOUND_TOPIC = "outbound-football-bets";
    private static final String TEST_PARTICIPANT_DATA_TOPIC = "participant-data";
    private static final String TEST_CONSUMER_GROUP = "fbe-test";
    private static final ObjectMapper objectMapper = new ObjectMapper();


    private static Producer<String, InboundFootballBet> betInboundProducer;
    private static Producer<String, ParticipantData> participantDataProducer;
    private static Consumer<String, OutboundFootballBet> betOutboundConsumer;

    public static void sendBetInbound(InboundFootballBet bet) {
        if (betInboundProducer == null) {
            throw new IllegalStateException("createProducer() was not called properly!");
        }

        log.info("Sending following bet to kafka {}", bet);
        betInboundProducer.send(new ProducerRecord<>(TEST_BET_INBOUND_TOPIC, bet));
    }

    public static void sendParticipantData(ParticipantData participantData) {
        if (participantDataProducer == null) {
            throw new IllegalStateException("createProducer() was not called properly!");
        }

        log.info("Sending following bet to kafka {}", participantData);
        participantDataProducer.send(new ProducerRecord<>(TEST_PARTICIPANT_DATA_TOPIC, participantData));
    }

    public static Optional<OutboundFootballBet> receiveOutboundBet(String betId) {
        if (betOutboundConsumer == null) {
            throw new IllegalStateException("createBetOutboundConsumer() was not called properly!");
        }

        for (int i = 0; i < MAX_RETRIES; i++) {
            log.info("{}/{} Waiting for a event with betID {} to appear in kafka", i + 1, MAX_RETRIES, betId);
            ConsumerRecords<String, OutboundFootballBet> consumerRecords = betOutboundConsumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) {
                for (ConsumerRecord<String, OutboundFootballBet> consumerRecord : consumerRecords) {
                    OutboundFootballBet message = consumerRecord.value();

                    if (message != null && betId.equals(message.getId())) {
                        log.info("Matching message found {}", message);
                        return Optional.of(message);
                    }
                }
            }
            log.info("Message not matching our query found");
        }

        log.error("No message for betID {} was found in kafka", betId);
        return Optional.empty();
    }

    public static void createBetInboundProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, (Class<KafkaSerializer<InboundFootballBet>>) ((Class) KafkaSerializer.class));

        betInboundProducer = new KafkaProducer<>(props);
    }

    public static void createParticipantDataProducer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, (Class<KafkaSerializer<ParticipantData>>) ((Class) KafkaSerializer.class));

        participantDataProducer = new KafkaProducer<>(props);
    }

    public static void createBetOutboundConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TEST_BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OutboundBetDeserializer.class);
        Consumer<String, OutboundFootballBet> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TEST_BET_OUTBOUND_TOPIC));

        //ensure that consumer is running
        consumer.poll(Duration.ofSeconds(1));

        betOutboundConsumer = consumer;
    }

    public static void closeBetInboundProducer() {
        if (betInboundProducer != null) {
            betInboundProducer.close();
        }
    }

    public static void closeParticipantProducer() {
        if (participantDataProducer != null) {
            participantDataProducer.close();
        }
    }

    public static void closeBetOutboundConsumer() {
        if (betOutboundConsumer != null) {
            betOutboundConsumer.close(Duration.ofSeconds(1));
        }
    }

    public static class OutboundBetDeserializer implements Deserializer<OutboundFootballBet> {

        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public OutboundFootballBet deserialize(String topic, byte[] bytes) {
            try {
                return objectMapper.readValue(bytes, OutboundFootballBet.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize test bet");
            }
        }

        @Override
        public void close() {
        }
    }

    public static class KafkaSerializer<T> implements Serializer<T> {

        @Override
        public byte[] serialize(String topic, T data) {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed when serializing an inbound bet.");
            }
        }
    }
}