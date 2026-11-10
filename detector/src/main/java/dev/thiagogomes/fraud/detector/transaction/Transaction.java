package dev.thiagogomes.fraud.detector.transaction;

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
    public Transaction {
        if (transactionId == null || cardId == null || cardId.isBlank() || cardId.length() > 64
                || amount == null || amount.signum() <= 0 || amount.scale() > 2
                || (long) amount.precision() - amount.scale() > 17 || timestamp == null
                || timestamp.isBefore(Instant.EPOCH) || timestamp.isAfter(Instant.ofEpochMilli(Long.MAX_VALUE))
                || !Double.isFinite(latitude) || Math.abs(latitude) > 90
                || !Double.isFinite(longitude) || Math.abs(longitude) > 180) {
            throw new IllegalArgumentException("Transação inválida: confira identificação, valor, coordenadas e event time");
        }
    }
}
