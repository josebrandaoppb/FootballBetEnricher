package com.flutter.enricher.flow.operators;

import com.flutter.enricher.model.bet.InboundBetParticipant;
import com.flutter.enricher.model.bet.InboundFootballBet;
import com.flutter.enricher.model.bet.VisitingCondition;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Transforms an InboundFootballBet into multiple InboundBetParticipant.
 * A simple flatMap.
 */
public class BetParticipantSplitter implements FlatMapFunction<InboundFootballBet, InboundBetParticipant> {

    @Override
    public void flatMap(InboundFootballBet inboundBet, Collector<InboundBetParticipant> collector) {
        InboundBetParticipant home = InboundBetParticipant.builder()
                .betId(inboundBet.getId())
                .outcome(inboundBet.getOutcome())
                .stake(inboundBet.getStake())
                .participantId(inboundBet.getHome().getId())
                .visitingCondition(VisitingCondition.HOME)
                .build();
        collector.collect(home);

        InboundBetParticipant away = InboundBetParticipant.builder()
                .betId(inboundBet.getId())
                .outcome(inboundBet.getOutcome())
                .stake(inboundBet.getStake())
                .participantId(inboundBet.getAway().getId())
                .visitingCondition(VisitingCondition.AWAY)
                .build();

        collector.collect(away);
    }
}
