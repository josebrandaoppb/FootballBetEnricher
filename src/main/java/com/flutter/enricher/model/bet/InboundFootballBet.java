package com.flutter.enricher.model.bet;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class InboundFootballBet {
    private String id;
    private Double stake;
    private String outcome;
    private InboundParticipantData home;
    private InboundParticipantData away;

    @Data
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InboundParticipantData {
        private String id;
    }
}
