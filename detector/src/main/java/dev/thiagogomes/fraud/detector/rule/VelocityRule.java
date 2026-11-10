package dev.thiagogomes.fraud.detector.rule;

import java.time.Clock;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import dev.thiagogomes.fraud.detector.alert.FraudAlert;
import dev.thiagogomes.fraud.detector.config.FraudRulesProperties;
import dev.thiagogomes.fraud.detector.transaction.Transaction;

public record VelocityRule(FraudRulesProperties.Velocity properties, Clock clock) {
    public KStream<String, FraudAlert> apply(StreamsBuilder builder, KStream<String, Transaction> transactions) {
        return new WindowedCountRule("velocity", "VELOCITY", (long) properties.maxTransactions() + 1,
                properties.window(), properties.grace(), clock).apply(builder, transactions);
    }
}
