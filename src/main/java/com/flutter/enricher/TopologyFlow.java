package com.flutter.enricher;

import com.flutter.enricher.model.bet.InboundBetParticipant;
import com.flutter.enricher.model.bet.InboundFootballBet;
import com.flutter.enricher.model.bet.OutboundBetParticipant;
import com.flutter.enricher.model.bet.OutboundFootballBet;
import com.flutter.enricher.model.participant.ParticipantData;
import com.flutter.enricher.flow.operators.BetParticipantEnricher;
import com.flutter.enricher.flow.operators.BetParticipantSplitter;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Where the application flow is defined, here we receive everything we need to implement the business logic.
 */
public class TopologyFlow {
    public static DataStream<OutboundFootballBet> topologyMainFlow(
            DataStream<InboundFootballBet> inboundBetStream,
            DataStream<ParticipantData> participantDataDataStream) {

        //split into bet participants
        final DataStream<InboundBetParticipant> inboundBetParticipantStream = inboundBetStream.flatMap(new BetParticipantSplitter())
                .name("BetParticipantSplitter").uid("BetParticipantSplitter")
                .keyBy(InboundBetParticipant::getParticipantId);

        //enrich with participant data
        final DataStream<OutboundBetParticipant> outboundBetParticipantStream = inboundBetParticipantStream
                .connect(participantDataDataStream.keyBy(ParticipantData::getId)) //connect to our participant stream(same key), we can use CoFunctions now
                .process(new BetParticipantEnricher())
                .name("BetParticipantEnricher").uid("BetParticipantEnricher");

        //merge bet participants into bet
        return outboundBetParticipantStream
                .keyBy(OutboundBetParticipant::getBetId)
                //TODO: Implement the BetParticipantMerger instead of this dummy map
                .map(outboundParticipant -> OutboundFootballBet.builder().build())
                .name("BetMerger").uid("BetMerger");
    }
}
