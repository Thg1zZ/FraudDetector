package dev.thiagogomes.fraud.detector.stream;

import java.time.Clock;
import java.time.Duration;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import dev.thiagogomes.fraud.detector.transaction.Transaction;

final class TransactionDeserializer implements Deserializer<Transaction> {
    static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(1);
    private final JacksonJsonDeserializer<Transaction> json =
            new JacksonJsonDeserializer<>(Transaction.class).ignoreTypeHeaders();
    private final Clock clock;

    TransactionDeserializer(Clock clock) {
        this.clock = clock;
    }

    @Override
    public Transaction deserialize(String topic, byte[] data) {
        return validate(json.deserialize(topic, data));
    }

    @Override
    public Transaction deserialize(String topic, Headers headers, byte[] data) {
        return validate(json.deserialize(topic, headers, data));
    }

    private Transaction validate(Transaction transaction) {
        // Antes do timestamp extractor: um evento futuro não pode avançar o stream time.
        if (transaction != null && transaction.timestamp().isAfter(clock.instant().plus(MAX_FUTURE_SKEW))) {
            throw new SerializationException("Event time excede a tolerância de um minuto no futuro");
        }
        return transaction;
    }

    @Override
    public void close() {
        json.close();
    }
}
