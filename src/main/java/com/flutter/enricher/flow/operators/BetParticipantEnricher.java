package com.flutter.enricher.flow.operators;

import com.flutter.enricher.model.bet.InboundBetParticipant;
import com.flutter.enricher.model.bet.OutboundBetParticipant;
import com.flutter.enricher.model.participant.ParticipantData;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Transforms an InboundBetParticipant into OutboundBetParticipant.
 * Consumes both InboundBetParticipant and ParticipantData streams.
 * Whenever an InboundBetParticipant is received checks if any ParticipantData for it exists and uses it to transform it in an OutboundBetParticipant.
 * Key: String - betId
 * Input1: InboundBetParticipant
 * Input2: ParticipantData
 * Output: OutboundBetParticipant
 */
public class BetParticipantEnricher extends KeyedCoProcessFunction<String, InboundBetParticipant, ParticipantData, OutboundBetParticipant> {

    private static final ValueStateDescriptor<ParticipantData> participantDataValueStateDescriptor =
            new ValueStateDescriptor<>("participantState", Types.POJO(ParticipantData.class));

    protected transient ValueState<ParticipantData> participantState;

    @Override
    public void open(Configuration parameters) {
        participantState = getRuntimeContext().getState(participantDataValueStateDescriptor);
    }

    @Override
    public void processElement1(InboundBetParticipant inboundBetParticipant, Context ctx, Collector<OutboundBetParticipant> collector) throws Exception {
        ParticipantData participantData = this.participantState.value();
        ParticipantData.Stats participantStats = participantData.getStats();

        collector.collect(OutboundBetParticipant.builder()
                .betId(inboundBetParticipant.getBetId())
                .stake(inboundBetParticipant.getStake())
                .outcome(inboundBetParticipant.getOutcome())
                .participantId(inboundBetParticipant.getParticipantId())
                .visitingCondition(inboundBetParticipant.getVisitingCondition())
                .name(participantData.getName())
                .players(participantData.getPlayers())
                .scored(participantStats.getScored())
                .yellows(participantStats.getYellows())
                .build());
    }

    @Override
    public void processElement2(ParticipantData participantData, Context ctx, Collector<OutboundBetParticipant> collector) throws Exception {
        //Assume we always get the full state in each message for a participant
        this.participantState.update(participantData);
    }
}
