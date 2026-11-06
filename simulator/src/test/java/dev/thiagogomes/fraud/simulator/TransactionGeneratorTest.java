package dev.thiagogomes.fraud.simulator;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

class TransactionGeneratorTest {

    private final Clock clock = Clock.fixed(Instant.parse("2026-11-06T12:00:00Z"), ZoneOffset.UTC);
    private final TransactionGenerator generator =
            new TransactionGenerator(new SimulatorProperties(20, 10, 0.0), new Random(42), clock);
    private final Card card = generator.cards().getFirst();

    @Test
    void normalTrafficStaysInHomeCity() {
        List<Transaction> batch = generator.nextBatch(50);

        assertThat(batch).hasSize(50);
        assertThat(batch).allSatisfy(tx -> {
            Card owner = generator.cards().stream().filter(c -> c.id().equals(tx.cardId())).findFirst().orElseThrow();
            assertThat(tx.city()).isEqualTo(owner.homeCity().name());
        });
    }

    @Test
    void velocityBurstHitsSameCardWithinOneMinute() {
        List<Transaction> burst = generator.generate(FraudScenario.VELOCITY, card);

        assertThat(burst).hasSizeGreaterThan(5).allMatch(tx -> tx.cardId().equals(card.id()));
        assertThat(span(burst)).isLessThan(Duration.ofMinutes(1));
    }

    @Test
    void highAmountIsFarAboveCardAverage() {
        Transaction tx = generator.generate(FraudScenario.HIGH_AMOUNT, card).getFirst();

        assertThat(tx.amount()).isGreaterThan(card.averageAmount().multiply(BigDecimal.valueOf(5)));
    }

    @Test
    void impossibleTravelUsesTwoDifferentCities() {
        List<Transaction> pair = generator.generate(FraudScenario.IMPOSSIBLE_TRAVEL, card);

        assertThat(pair).hasSize(2);
        assertThat(pair.get(0).city()).isNotEqualTo(pair.get(1).city());
        assertThat(span(pair)).isLessThan(Duration.ofMinutes(1));
    }

    @Test
    void cardTestingSendsManySmallAmounts() {
        List<Transaction> burst = generator.generate(FraudScenario.CARD_TESTING, card);

        assertThat(burst).hasSizeGreaterThanOrEqualTo(5)
                .allMatch(tx -> tx.amount().compareTo(BigDecimal.valueOf(5)) < 0);
        assertThat(span(burst)).isLessThan(Duration.ofMinutes(2));
    }

    private static Duration span(List<Transaction> transactions) {
        return Duration.between(transactions.getFirst().timestamp(), transactions.getLast().timestamp());
    }
}
