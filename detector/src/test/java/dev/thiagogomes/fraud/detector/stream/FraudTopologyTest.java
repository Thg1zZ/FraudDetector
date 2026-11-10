package dev.thiagogomes.fraud.detector.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import dev.thiagogomes.fraud.detector.alert.FraudAlert;
import dev.thiagogomes.fraud.detector.config.FraudRulesProperties;
import dev.thiagogomes.fraud.detector.transaction.Transaction;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class FraudTopologyTest {

    private static final Instant BASE = Instant.parse("2026-11-09T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(BASE.plus(Duration.ofDays(2)), ZoneOffset.UTC);

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, Transaction> input;
    private TestOutputTopic<String, FraudAlert> output;
    private long sequence;
    private SimpleMeterRegistry meters;

    @BeforeEach
    void setUp() {
        start(new FraudRulesProperties.Velocity(5, Duration.ofMinutes(1), Duration.ofSeconds(30)));
    }

    private void start(FraudRulesProperties.Velocity velocity) {
        StreamsBuilder builder = new StreamsBuilder();
        meters = new SimpleMeterRegistry();
        new FraudTopology().fraudAlerts(builder, new FraudRulesProperties(velocity,
                new FraudRulesProperties.HighAmount(new BigDecimal("5"), 5),
                new FraudRulesProperties.ImpossibleTravel(900),
                new FraudRulesProperties.CardTesting(new BigDecimal("5"), 5, Duration.ofMinutes(2), Duration.ofSeconds(30))), CLOCK,
                new FraudMetrics(meters));
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-topology-test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        config.put("deserialization.exception.handler", "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        config.put("errors.dead.letter.queue.topic.name", FraudTopology.TRANSACTIONS_DLT);
        driver = new TopologyTestDriver(builder.build(), config);
        var transactionSerde = new JacksonJsonSerde<>(Transaction.class).noTypeInfo().ignoreTypeHeaders();
        var alertSerde = new JacksonJsonSerde<>(FraudAlert.class).noTypeInfo().ignoreTypeHeaders();
        alertSerde.deserializer().dontRemoveTypeHeaders();
        input = driver.createInputTopic(FraudTopology.TRANSACTIONS_TOPIC,
                Serdes.String().serializer(), transactionSerde.serializer());
        output = driver.createOutputTopic(FraudTopology.ALERTS_TOPIC,
                Serdes.String().deserializer(), alertSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void fiveTransactionsDoNotExceedTheLimit() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", i * 1000);
        }

        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void recordsTransactionsAlertsAndNonNegativeLatency() {
        for (int i = 0; i < 6; i++) send("card-0042", 172830000 + i * 1000);
        assertThat(meters.get("fraud.transactions").counter().count()).isEqualTo(6);
        assertThat(meters.get("fraud.alerts").tag("rule", "VELOCITY").counter().count()).isEqualTo(1);
        assertThat(meters.get("fraud.detection.latency").timer().count()).isEqualTo(1);
        assertThat(meters.get("fraud.detection.latency").timer().totalTime(java.util.concurrent.TimeUnit.SECONDS)).isZero();
    }

    @Test
    void sixthTransactionEmitsAnAlertWithTheOriginalTransactionAndInjectedClock() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", i * 1000);
        }
        Transaction sixth = send("card-0042", 5000);

        var record = output.readRecord();
        FraudAlert alert = record.value();
        assertThat(record.key()).isEqualTo("card-0042");
        assertThat(record.headers().lastHeader("__TypeId__")).isNull();
        assertThat(alert.id()).isNotNull();
        assertThat(alert.rule()).isEqualTo("VELOCITY");
        assertThat(alert.transactionId()).isEqualTo(sixth.transactionId());
        assertThat(alert.cardId()).isEqualTo(sixth.cardId());
        assertThat(alert.amount()).isEqualByComparingTo(sixth.amount());
        assertThat(alert.transactionAt()).isEqualTo(sixth.timestamp());
        assertThat(alert.detectedAt()).isEqualTo(CLOCK.instant());
        assertThat(((Number) alert.details().get("count")).longValue()).isEqualTo(6);
        assertThat(alert.details()).containsKeys("maxTransactions", "windowStart", "windowEnd");
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void burstProducesOneAlertPerExcessTransactionDespiteOverlappingWindows() {
        List<Transaction> burst = java.util.stream.IntStream.range(0, 8)
                .mapToObj(i -> send("card-0042", i * 500)).toList();

        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).extracting(FraudAlert::transactionId)
                .containsExactly(burst.get(5).transactionId(), burst.get(6).transactionId(),
                        burst.get(7).transactionId());
        assertThat(alerts).extracting(FraudAlert::id).doesNotHaveDuplicates();

        send("card-0042", 65000);
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void countsAreIsolatedByCard() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", i * 1000);
            send("card-0043", i * 1000);
        }
        assertThat(output.isEmpty()).isTrue();

        send("card-0042", 5000);
        assertThat(output.readValuesToList()).singleElement()
                .extracting(FraudAlert::cardId).isEqualTo("card-0042");
    }

    @Test
    void detectsABurstAcrossTheWallClockMinuteBoundary() {
        for (long second : new long[] {55, 56, 57, 60, 61, 62}) {
            send("card-0042", second * 1000);
        }

        assertThat(output.readValuesToList()).hasSize(1);
    }

    @Test
    void includesTransactionsExactlyOneMinuteApart() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", 0);
        }
        send("card-0042", 60000);

        assertThat(output.readValuesToList()).hasSize(1);
    }

    @Test
    void excludesTransactionsMoreThanOneMinuteApart() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", 0);
        }
        send("card-0042", 60001);

        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void usesPayloadEventTimeInsteadOfKafkaRecordTimestamp() {
        for (int i = 0; i < 6; i++) {
            Transaction transaction = transaction("card-0042", i * 1000);
            input.pipeInput(transaction.cardId(), transaction, BASE.plusSeconds(i * 120));
        }

        assertThat(output.readValuesToList()).hasSize(1);
    }

    @Test
    void acceptsAnOutOfOrderTransactionInAnOpenWindow() {
        for (long second : new long[] {0, 10, 20, 30, 40}) {
            send("card-0042", second * 1000);
        }
        Transaction late = send("card-0042", 15000);

        assertThat(output.readValuesToList()).singleElement()
                .extracting(FraudAlert::transactionId).isEqualTo(late.transactionId());
    }

    @Test
    void acceptsLateDataAfterWindowEndWhileWithinGrace() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", 60000);
        }
        send("card-0043", 80000);
        Transaction late = send("card-0042", 60000);

        assertThat(output.readValuesToList()).singleElement()
                .extracting(FraudAlert::transactionId).isEqualTo(late.transactionId());
    }

    @Test
    void dropsLateDataAfterAllItsWindowsAndGraceHaveClosed() {
        for (int i = 0; i < 5; i++) {
            send("card-0042", 60000);
        }
        send("card-0043", 91001);
        send("card-0042", 60000);

        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void continuesAlertingOncePerExcessTransactionAsOldWindowsExpire() {
        List<Transaction> traffic = java.util.stream.IntStream.range(0, 25)
                .mapToObj(i -> send("card-0042", i * 10000)).toList();

        assertThat(output.readValuesToList()).extracting(FraudAlert::transactionId)
                .containsExactlyElementsOf(traffic.subList(5, traffic.size()).stream()
                        .map(Transaction::transactionId).toList());
    }

    @Test
    void honorsConfiguredThresholdAndWindow() {
        driver.close();
        start(new FraudRulesProperties.Velocity(2, Duration.ofSeconds(10), Duration.ofSeconds(5)));

        send("card-0042", 0);
        send("card-0042", 10001);
        send("card-0042", 11000);
        assertThat(output.isEmpty()).isTrue();
        send("card-0042", 12000);

        assertThat(output.readValuesToList()).hasSize(1);
    }

    @Test
    void ignoresTombstonesAndQuarantinesNullKeys() {
        input.pipeInput("card-0042", null, BASE);
        input.pipeInput(null, transaction("card-0042", 0), BASE);
        for (int i = 0; i < 5; i++) {
            send("card-0042", i * 1000);
        }

        assertThat(output.isEmpty()).isTrue();
        var rejected = driver.createOutputTopic(FraudTopology.TRANSACTIONS_DLT,
                Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertThat(rejected.readRecord().key()).isNull();
    }

    @Test
    void rejectsFuturePoisonAndMismatchedKeysWithoutAdvancingRuleWindows() {
        input.pipeInput("card-0042", transaction("card-0042", 0), BASE);
        input.pipeInput("card-0042", transaction("card-0042", Duration.ofDays(365).toMillis()), BASE);
        input.pipeInput("wrong-key", transaction("card-0042", 1000), BASE);
        for (int i = 1; i < 6; i++) send("card-0042", i * 1000);
        assertThat(output.readValuesToList()).singleElement().extracting(FraudAlert::rule).isEqualTo("VELOCITY");
        var rejected = driver.createOutputTopic(FraudTopology.TRANSACTIONS_DLT,
                Serdes.String().deserializer(), Serdes.ByteArray().deserializer());
        assertThat(rejected.readValuesToList()).hasSize(2);
        assertThat(meters.get("fraud.transactions").counter().count()).isEqualTo(6);
    }

    @Test
    void readsTheSimulatorJsonContractWithoutJavaTypeHeaders() {
        TestInputTopic<String, String> jsonInput = driver.createInputTopic(FraudTopology.TRANSACTIONS_TOPIC,
                Serdes.String().serializer(), Serdes.String().serializer());
        for (int i = 0; i < 6; i++) {
            String json = """
                    {"transactionId":"%s","cardId":"card-0042","amount":89.97,
                     "merchant":"Supermercado","city":"Curitiba","latitude":-25.43,
                     "longitude":-49.27,"timestamp":"%s"}
                    """.formatted(new UUID(0, i), BASE.plusSeconds(i));
            jsonInput.pipeInput("card-0042", json, BASE);
        }

        assertThat(output.readValuesToList()).singleElement()
                .extracting(FraudAlert::transactionId).isEqualTo(new UUID(0, 5));
    }

    @Test
    void highAmountNeedsFivePreviousTransactionsAndExcludesTheCurrentPurchaseFromItsAverage() {
        for (int i = 0; i < 4; i++) purchase("card-few", "10", i * 120, 0, 0);
        purchase("card-few", "1000", 500, 0, 0);
        assertThat(alertsFor("HIGH_AMOUNT")).isEmpty();

        for (int i = 0; i < 5; i++) purchase("card-ready", "10", 600 + i * 120, 0, 0);
        Transaction high = purchase("card-ready", "50.01", 1200, 0, 0);
        assertThat(alertsFor("HIGH_AMOUNT")).singleElement().satisfies(alert -> {
            assertThat(alert.transactionId()).isEqualTo(high.transactionId());
            assertThat(new BigDecimal(alert.details().get("averageAmount").toString())).isEqualByComparingTo("10");
            assertThat(((Number) alert.details().get("historyCount")).longValue()).isEqualTo(5);
        });
    }

    @Test
    void highAmountDoesNotAlertAtExactlyFiveTimesTheAverage() {
        for (int i = 0; i < 5; i++) purchase("card-0042", "10", i * 120, 0, 0);
        purchase("card-0042", "50", 600, 0, 0);
        assertThat(alertsFor("HIGH_AMOUNT")).isEmpty();
    }

    @Test
    void latePurchasesDoNotContaminateTheHistoricalAverage() {
        for (int i = 0; i < 5; i++) purchase("card-0042", "10", i * 120, 0, 0);
        purchase("card-0042", "100000", 1, 0, 0);
        Transaction high = purchase("card-0042", "51", 600, 0, 0);
        assertThat(alertsFor("HIGH_AMOUNT")).singleElement()
                .extracting(FraudAlert::transactionId).isEqualTo(high.transactionId());
    }

    @Test
    void impossibleTravelDetectsFastTravelAndAllowsEnoughTime() {
        purchase("card-fast", "10", 0, -25.43, -49.27);
        Transaction far = purchase("card-fast", "10", 30, 40.71, -74.00);
        purchase("card-slow", "10", 0, -25.43, -49.27);
        purchase("card-slow", "10", 86400, 40.71, -74.00);
        assertThat(alertsFor("IMPOSSIBLE_TRAVEL")).singleElement()
                .extracting(FraudAlert::transactionId).isEqualTo(far.transactionId());
    }

    @Test
    void simultaneousPurchasesOnlyAlertWhenTheirLocationsDiffer() {
        purchase("card-0042", "10", 0, 0, 0);
        purchase("card-0042", "10", 0, 0, 0);
        assertThat(alertsFor("IMPOSSIBLE_TRAVEL")).isEmpty();
        purchase("card-0042", "10", 0, 1, 1);
        assertThat(alertsFor("IMPOSSIBLE_TRAVEL")).singleElement().satisfies(alert ->
                assertThat(((Number) alert.details().get("elapsedMillis")).longValue()).isZero());
    }

    @Test
    void lateLocationDoesNotReplaceTheMostRecentLocation() {
        purchase("card-0042", "10", 120, 40, -74);
        purchase("card-0042", "10", 90, -25, -49);
        purchase("card-0042", "10", 121, 40, -74);
        assertThat(alertsFor("IMPOSSIBLE_TRAVEL")).isEmpty();
    }

    @Test
    void haversineHandlesTheDateLineAndAntipodalLocations() {
        assertThat(dev.thiagogomes.fraud.detector.rule.ImpossibleTravelRule.distanceKm(0, 179, 0, -179))
                .isBetween(222.0, 223.0);
        assertThat(dev.thiagogomes.fraud.detector.rule.ImpossibleTravelRule.distanceKm(0, 0, 0, 180))
                .isBetween(20015.0, 20016.0);
    }

    @Test
    void cardTestingCountsOnlyAmountsStrictlyBelowFiveAndIncludesTwoMinuteBoundary() {
        for (int i = 0; i < 4; i++) purchase("card-0042", "4.99", 0, 0, 0);
        purchase("card-0042", "5.00", 10, 0, 0);
        assertThat(alertsFor("CARD_TESTING")).isEmpty();
        Transaction fifth = purchase("card-0042", "4.99", 120, 0, 0);
        assertThat(alertsFor("CARD_TESTING")).singleElement()
                .extracting(FraudAlert::transactionId).isEqualTo(fifth.transactionId());
    }

    @Test
    void cardTestingExpiresOldPurchasesAndKeepsCardsSeparate() {
        for (int i = 0; i < 4; i++) purchase("card-0042", "1", 0, 0, 0);
        purchase("card-0043", "1", 1, 0, 0);
        purchase("card-0042", "1", 121, 0, 0);
        assertThat(alertsFor("CARD_TESTING")).isEmpty();
    }

    @Test
    void cardTestingDeduplicatesOverlappingWindowsAndAcceptsLateDataWithinGrace() {
        for (int i = 0; i < 5; i++) purchase("card-0042", "1", 120, 0, 0);
        assertThat(alertsFor("CARD_TESTING")).hasSize(1);
        purchase("card-0043", "1", 140, 0, 0);
        purchase("card-0042", "1", 120, 0, 0);
        assertThat(alertsFor("CARD_TESTING")).hasSize(1);
        purchase("card-0043", "1", 400, 0, 0);
        purchase("card-0042", "1", 120, 0, 0);
        assertThat(alertsFor("CARD_TESTING")).isEmpty();
    }

    @Test
    void oneTransactionCanTriggerMultipleRulesWithDifferentStableAlertIds() {
        for (int i = 0; i < 5; i++) purchase("card-0042", "10", i, 0, 0);
        Transaction high = purchase("card-0042", "100", 5, 1, 1);
        List<FraudAlert> alerts = output.readValuesToList();
        assertThat(alerts).extracting(FraudAlert::rule)
                .containsExactlyInAnyOrder("VELOCITY", "HIGH_AMOUNT", "IMPOSSIBLE_TRAVEL");
        assertThat(alerts).allMatch(alert -> alert.transactionId().equals(high.transactionId()));
        assertThat(alerts).extracting(FraudAlert::id).doesNotHaveDuplicates();
        alerts.forEach(alert -> assertThat(FraudAlert.create(high, alert.rule(), alert.details(), CLOCK).id())
                .isEqualTo(alert.id()));
    }

    private List<FraudAlert> alertsFor(String rule) {
        return output.readValuesToList().stream().filter(alert -> alert.rule().equals(rule)).toList();
    }

    private Transaction purchase(String cardId, String amount, long seconds, double latitude, double longitude) {
        Transaction transaction = new Transaction(new UUID(1, ++sequence), cardId, new BigDecimal(amount),
                "Loja", "Cidade", latitude, longitude, BASE.plusSeconds(seconds));
        input.pipeInput(cardId, transaction, transaction.timestamp());
        return transaction;
    }

    private Transaction send(String cardId, long offsetMillis) {
        Transaction transaction = transaction(cardId, offsetMillis);
        input.pipeInput(cardId, transaction, transaction.timestamp());
        return transaction;
    }

    private Transaction transaction(String cardId, long offsetMillis) {
        return new Transaction(new UUID(0, ++sequence), cardId, new BigDecimal("89.97"), "Supermercado", "Curitiba",
                -25.43, -49.27, BASE.plusMillis(offsetMillis));
    }
}
