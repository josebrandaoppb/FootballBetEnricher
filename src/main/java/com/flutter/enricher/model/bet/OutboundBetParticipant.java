package com.flutter.enricher.model.bet;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Participant of a given bet, it will carry properties common to the bet until the end of the stream
 * Another alternative could be to split the bet details as well
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OutboundBetParticipant {
    //Bet level properties
    private String betId;
    private Double stake;
    private String outcome;

    //Participant properties
    private String participantId;
    private VisitingCondition visitingCondition;
    private String name;
    private Integer scored;
    private Integer yellows;
    private List<String> players;
}
