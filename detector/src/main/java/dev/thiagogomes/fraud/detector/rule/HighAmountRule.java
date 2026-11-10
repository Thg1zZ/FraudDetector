package dev.thiagogomes.fraud.detector.rule;

import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import dev.thiagogomes.fraud.detector.alert.FraudAlert;
import dev.thiagogomes.fraud.detector.config.FraudRulesProperties;
import dev.thiagogomes.fraud.detector.transaction.Transaction;

public record HighAmountRule(FraudRulesProperties.HighAmount properties, Clock clock) {
    private static final String STORE = "high-amount-history";

    public KStream<String, FraudAlert> apply(StreamsBuilder builder, KStream<String, Transaction> transactions) {
        builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE), Serdes.String(),
                new JacksonJsonSerde<>(History.class).noTypeInfo().ignoreTypeHeaders()));
        return transactions.processValues(() -> new ContextualFixedKeyProcessor<String, Transaction, FraudAlert>() {
            private KeyValueStore<String, History> history;

            @Override
            public void init(FixedKeyProcessorContext<String, FraudAlert> context) {
                super.init(context);
                history = context.getStateStore(STORE);
            }

            @Override
            public void process(FixedKeyRecord<String, Transaction> record) {
                Transaction transaction = record.value();
                History previous = history.get(record.key());
                if (previous != null && transaction.timestamp().isBefore(previous.timestamp())) return;
                if (previous != null && previous.count() >= properties.minHistory()
                        && transaction.amount().multiply(BigDecimal.valueOf(previous.count()))
                        .compareTo(previous.total().multiply(properties.multiplier())) > 0) {
                    context().forward(record.withValue(FraudAlert.create(transaction, "HIGH_AMOUNT", Map.of(
                            "averageAmount", previous.total().divide(BigDecimal.valueOf(previous.count()), MathContext.DECIMAL128),
                            "historyCount", previous.count(), "multiplier", properties.multiplier()), clock)));
                }
                history.put(record.key(), previous == null
                        ? new History(1, transaction.amount(), transaction.timestamp())
                        : new History(previous.count() + 1, previous.total().add(transaction.amount()), transaction.timestamp()));
            }
        }, STORE);
    }

    public record History(long count, BigDecimal total, Instant timestamp) { }
}
