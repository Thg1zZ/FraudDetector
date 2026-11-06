package dev.thiagogomes.fraud.simulator;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record Transaction(
        UUID transactionId,
        String cardId,
        BigDecimal amount,
        String merchant,
        String city,
        double latitude,
        double longitude,
        Instant timestamp) {
}
