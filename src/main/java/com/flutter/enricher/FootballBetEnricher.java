package com.flutter.enricher;

import com.flutter.enricher.configuration.EnvironmentConfig;
import com.flutter.enricher.flow.sink.OutboundBetStream;
import com.flutter.enricher.flow.source.InboundBetStream;
import com.flutter.enricher.flow.source.ParticipantDataStream;
import com.flutter.enricher.model.bet.InboundFootballBet;
import com.flutter.enricher.model.bet.OutboundFootballBet;
import com.flutter.enricher.model.participant.ParticipantData;
import com.typesafe.sslconfig.util.ConfigLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Entry Point of the application
 * Loads properties, configures environment and executes the application
 */
@Slf4j
public class FootballBetEnricher {

    private static final String PROPERTIES_FILE_LOCATION = "/application.properties";

    public static void main(String[] args) throws Exception {
        log.info("Loading...");
        //prepare the environment
        final ParameterTool propertiesFileConfig = ParameterTool.fromPropertiesFile(ConfigLoader.class.getResourceAsStream(PROPERTIES_FILE_LOCATION));
        final StreamExecutionEnvironment env = EnvironmentConfig.configureEnvironment(propertiesFileConfig);

        // initiate a inbound bet stream
        final FlinkKafkaConsumer<InboundFootballBet> betConsumer = InboundBetStream.get(propertiesFileConfig);
        final DataStream<InboundFootballBet> inboundBetStream = env.addSource(betConsumer)
                .name("InboundBetConsumer").uid("InboundBetConsumer");

        // initiate a inbound participant stream
        final FlinkKafkaConsumer<ParticipantData> participantDataConsumer = ParticipantDataStream.get(propertiesFileConfig);
        final DataStream<ParticipantData> participantDataStream = env.addSource(participantDataConsumer)
                .name("ParticipantDataConsumer").uid("ParticipantDataConsumer");

        // apply domain logic and return the expected output
        final DataStream<OutboundFootballBet> outboundBetStream =
                TopologyFlow.topologyMainFlow(inboundBetStream, participantDataStream);

        // produce the output to a kafka sink
        outboundBetStream.addSink(OutboundBetStream.get(propertiesFileConfig))
                .name("OutboundBetSink").uid("OutboundBetSink");

        participantDataStream.print().setParallelism(1);
        inboundBetStream.print().setParallelism(1);
        outboundBetStream.print().setParallelism(1);

        //Execute our job
        log.info("Starting application, params={}", propertiesFileConfig.getProperties());
        env.execute("FootballBetEnricher");

    }
}
