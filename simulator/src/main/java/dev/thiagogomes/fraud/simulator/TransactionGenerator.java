package dev.thiagogomes.fraud.simulator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.random.RandomGenerator;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TransactionGenerator {

    private static final Logger log = LoggerFactory.getLogger(TransactionGenerator.class);

    private static final List<String> MERCHANTS = List.of(
            "Supermercado", "Farmácia", "Posto de Combustível", "Restaurante", "Loja Online", "Eletrônicos");

    private static final int VELOCITY_BURST = 8;
    private static final int CARD_TESTING_BURST = 6;

    private final RandomGenerator random;
    private final Clock clock;
    private final double fraudRate;
    private final List<Card> cards;

    public TransactionGenerator(SimulatorProperties properties, RandomGenerator random, Clock clock) {
        this.random = random;
        this.clock = clock;
        this.fraudRate = properties.fraudRate();
        this.cards = IntStream.rangeClosed(1, properties.cards()).mapToObj(this::newCard).toList();
    }

    public List<Transaction> nextBatch(int size) {
        List<Transaction> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Card card = randomCard();
            if (random.nextDouble() < fraudRate) {
                FraudScenario scenario = randomElement(List.of(FraudScenario.values()));
                log.info("Fraude injetada: {} no cartão {}", scenario, card.id());
                batch.addAll(generate(scenario, card));
            } else {
                batch.add(transaction(card, card.homeCity(), typicalAmount(card), now()));
            }
        }
        return batch;
    }

    List<Transaction> generate(FraudScenario scenario, Card card) {
        Instant start = now();
        return switch (scenario) {
            case VELOCITY -> IntStream.range(0, VELOCITY_BURST)
                    .mapToObj(i -> transaction(card, card.homeCity(), typicalAmount(card), start.plusMillis(i * 500L)))
                    .toList();
            case HIGH_AMOUNT -> List.of(
                    transaction(card, card.homeCity(), multiply(card.averageAmount(), 8 + random.nextDouble() * 4), start));
            case IMPOSSIBLE_TRAVEL -> List.of(
                    transaction(card, card.homeCity(), typicalAmount(card), start),
                    transaction(card, distantCity(card.homeCity()), typicalAmount(card), start.plusSeconds(30)));
            case CARD_TESTING -> IntStream.range(0, CARD_TESTING_BURST)
                    .mapToObj(i -> transaction(card, card.homeCity(), smallAmount(), start.plusSeconds(i * 2L)))
                    .toList();
        };
    }

    List<Card> cards() {
        return cards;
    }

    private Card newCard(int number) {
        BigDecimal averageAmount = BigDecimal.valueOf(20 + random.nextDouble() * 480).setScale(2, RoundingMode.HALF_EVEN);
        return new Card("card-%04d".formatted(number), randomElement(City.ALL), averageAmount);
    }

    private Transaction transaction(Card card, City city, BigDecimal amount, Instant timestamp) {
        return new Transaction(UUID.randomUUID(), card.id(), amount, randomElement(MERCHANTS),
                city.name(), city.latitude(), city.longitude(), timestamp);
    }

    private BigDecimal typicalAmount(Card card) {
        return multiply(card.averageAmount(), 0.5 + random.nextDouble());
    }

    private BigDecimal smallAmount() {
        return BigDecimal.valueOf(1 + random.nextDouble() * 3.99).setScale(2, RoundingMode.DOWN);
    }

    private static BigDecimal multiply(BigDecimal value, double factor) {
        return value.multiply(BigDecimal.valueOf(factor)).setScale(2, RoundingMode.HALF_EVEN);
    }

    private City distantCity(City home) {
        List<City> others = City.ALL.stream().filter(city -> !city.equals(home)).toList();
        return randomElement(others);
    }

    private Card randomCard() {
        return randomElement(cards);
    }

    private <T> T randomElement(List<T> items) {
        return items.get(random.nextInt(items.size()));
    }

    private Instant now() {
        return Instant.now(clock);
    }
}
