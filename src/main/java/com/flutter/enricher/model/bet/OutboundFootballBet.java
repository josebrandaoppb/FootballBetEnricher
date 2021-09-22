package com.flutter.enricher.model.bet;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class OutboundFootballBet {
    private String id;
    private Double stake;
    private String outcome;
    private OutboundParticipantData home;
    private OutboundParticipantData away;

    @Data
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OutboundParticipantData {
        private String id;
        private String name;
        private List<String> players;
        private OutboundParticipantData.Stats stats;

        @Data
        @Builder(toBuilder = true)
        @NoArgsConstructor
        @AllArgsConstructor
        public static class Stats {
            private Integer scored;
            private Integer yellows;
        }
    }


}
