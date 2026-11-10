package dev.thiagogomes.fraud.detector.alert;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.time.Clock;

import dev.thiagogomes.fraud.detector.transaction.Transaction;

public record FraudAlert(
        UUID id,
        UUID transactionId,
        String cardId,
        String rule,
        BigDecimal amount,
        Map<String, Object> details,
        Instant transactionAt,
        Instant detectedAt) {
    public FraudAlert {
        if (id == null || transactionId == null || cardId == null || cardId.isBlank() || cardId.length() > 64
                || rule == null || amount == null || amount.signum() <= 0 || amount.scale() > 2
                || (long) amount.precision() - amount.scale() > 17 || details == null
                || transactionAt == null || detectedAt == null || transactionAt.isBefore(Instant.EPOCH)
                || detectedAt.isBefore(Instant.EPOCH) || transactionAt.getEpochSecond() > 253402300799L
                || detectedAt.getEpochSecond() > 253402300799L) {
            throw new IllegalArgumentException("Alerta inválido: identificação, valor, detalhes ou datas fora do contrato");
        }
        FraudRule.valueOf(rule);
    }

    public static FraudAlert create(Transaction transaction, String rule, Map<String, Object> details, Clock clock) {
        UUID id = UUID.nameUUIDFromBytes((transaction.transactionId() + ":" + rule).getBytes(StandardCharsets.UTF_8));
        return new FraudAlert(id, transaction.transactionId(), transaction.cardId(), rule, transaction.amount(),
                details, transaction.timestamp(), clock.instant());
    }
}
