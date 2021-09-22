package com.flutter.enricher.acceptance;

import com.flutter.enricher.acceptance.helper.KafkaHelper;
import com.flutter.enricher.model.bet.InboundFootballBet;
import com.flutter.enricher.model.bet.OutboundFootballBet;
import com.flutter.enricher.model.participant.ParticipantData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Support for the exercises.
 * Performs an acceptance test for the happy path of our enrichment.
 */
@Slf4j
public class EnrichmentAcceptanceTest {
    @BeforeAll
    static void setUp() {
        KafkaHelper.createBetInboundProducer();
        KafkaHelper.createParticipantDataProducer();
        KafkaHelper.createBetOutboundConsumer();
    }

    @AfterAll
    static void cleanUp() {
        KafkaHelper.closeBetInboundProducer();
        KafkaHelper.closeParticipantProducer();
        KafkaHelper.closeBetOutboundConsumer();
    }

    @Test
    public void shouldEnrichAFootballBet() throws InterruptedException {
        //Given Participant Data for participant 10 and 20
        List<String> homePlayers = List.of("Cristiano Ronaldo", "Messi", "Mbappe", "Neymar", "Seferovic");
        String homeId = "10";
        String homeName = "Benfica";
        int homeScored = 1354605;
        int homeYellows = 2;
        ParticipantData participant10 = ParticipantData.builder()
                .id(homeId)
                .name(homeName)
                .players(homePlayers)
                .stats(ParticipantData.Stats.builder()
                        .scored(homeScored)
                        .yellows(homeYellows)
                        .build())
                .build();

        List<String> awayPlayers = List.of("Nanu", "Carraça", "Manafa", "Zaidu", "Claudio Ramos");
        int awayYellows = 1303030;
        String awayName = "Porto";
        int awayGoals = 0;
        String awayId = "20";
        ParticipantData participant20 = ParticipantData.builder()
                .id(awayId)
                .name(awayName)
                .players(awayPlayers)
                .stats(ParticipantData.Stats.builder()
                        .scored(awayGoals)
                        .yellows(awayYellows)
                        .build())
                .build();

        KafkaHelper.sendParticipantData(participant10);
        KafkaHelper.sendParticipantData(participant20);

        log.info("Waiting a bit while participant data is injected...");
        Thread.sleep(3000);

        //When an Inbound Bet is received with participant 10 and 20
        String betId = "1234";
        double stake = 100.0;
        String outcome = "1";
        InboundFootballBet inboundBet = InboundFootballBet.builder()
                .id(betId)
                .stake(stake)
                .outcome(outcome)
                .home(InboundFootballBet.InboundParticipantData.builder()
                        .id(homeId)
                        .build())
                .away(InboundFootballBet.InboundParticipantData.builder()
                        .id(awayId)
                        .build())
                .build();
        KafkaHelper.sendBetInbound(inboundBet);

        //Then an Outbound Enriched Football Bet is expected
        Optional<OutboundFootballBet> outboundBetOpt = KafkaHelper.receiveOutboundBet(betId);
        assertTrue(outboundBetOpt.isPresent());
        OutboundFootballBet outboundFootballBet = outboundBetOpt.get();
        OutboundFootballBet.OutboundParticipantData home = outboundFootballBet.getHome();
        OutboundFootballBet.OutboundParticipantData away = outboundFootballBet.getAway();

        assertEquals(betId, outboundFootballBet.getId());
        assertEquals(100D, outboundFootballBet.getStake());
        assertEquals(outcome, outboundFootballBet.getOutcome());

        assertEquals(homeId, home.getId());
        assertEquals(homeName, home.getName());
        assertTrue(home.getPlayers().containsAll(homePlayers));
        assertEquals(homeScored, home.getStats().getScored());
        assertEquals(homeYellows, home.getStats().getYellows());

        assertEquals(awayId, away.getId());
        assertEquals(awayName, away.getName());
        assertTrue(away.getPlayers().containsAll(awayPlayers));
        assertEquals(awayGoals, away.getStats().getScored());
        assertEquals(awayYellows, away.getStats().getYellows());
    }
}
