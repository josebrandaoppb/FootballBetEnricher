package com.flutter.enricher.model.participant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class ParticipantData {
    private String id;
    private String name;
    private List<String> players;
    private Stats stats;

    @Data
    @Builder(toBuilder = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Stats {
        private Integer scored;
        private Integer yellows;
    }
}
