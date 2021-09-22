package com.flutter.enricher.model.bet;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Participant of a given bet, it will carry properties common to the bet until the end of the stream
 * Another alternative could be to split the bet details as well
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class InboundBetParticipant {
    private String betId;
    private Double stake;
    private String outcome;
    private String participantId;
    private VisitingCondition visitingCondition;
}
