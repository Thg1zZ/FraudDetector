package dev.thiagogomes.fraud.detector.stream;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import dev.thiagogomes.fraud.detector.transaction.Transaction;

class TransactionDeserializerTest {
    private static final Instant NOW = Instant.parse("2026-11-10T12:00:00Z");

    @Test
    void acceptsTheSkewBoundaryAndRejectsOneMillisecondBeyondBeforeTimestampExtraction() {
        try (var deserializer = new TransactionDeserializer(Clock.fixed(NOW, ZoneOffset.UTC));
             var serializer = new JacksonJsonSerializer<Transaction>().noTypeInfo()) {
            var boundary = transaction(NOW.plusSeconds(60));
            assertThat(deserializer.deserialize("transactions", serializer.serialize("transactions", boundary))).isEqualTo(boundary);
            assertThatThrownBy(() -> deserializer.deserialize("transactions", new RecordHeaders(),
                    serializer.serialize("transactions", transaction(NOW.plusSeconds(60).plusMillis(1)))))
                    .isInstanceOf(SerializationException.class);
            assertThat(deserializer.deserialize("transactions", (byte[]) null)).isNull();
            assertThatThrownBy(() -> deserializer.deserialize("transactions", new byte[] {'{'}))
                    .isInstanceOf(SerializationException.class);
        }
    }

    private Transaction transaction(Instant timestamp) {
        return new Transaction(new UUID(0, 1), "card-test", BigDecimal.ONE, "Loja", "Cidade", 0, 0, timestamp);
    }
}
