package dev.thiagogomes.fraud.detector.rule;

import java.time.Clock;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import dev.thiagogomes.fraud.detector.alert.FraudAlert;
import dev.thiagogomes.fraud.detector.config.FraudRulesProperties;
import dev.thiagogomes.fraud.detector.transaction.Transaction;

public record CardTestingRule(FraudRulesProperties.CardTesting properties, Clock clock) {
    public KStream<String, FraudAlert> apply(StreamsBuilder builder, KStream<String, Transaction> transactions) {
        return new WindowedCountRule("card-testing", "CARD_TESTING", properties.minTransactions(),
                properties.window(), properties.grace(), clock).apply(builder,
                transactions.filter((key, transaction) -> transaction.amount().compareTo(properties.maxAmount()) < 0));
    }
}
