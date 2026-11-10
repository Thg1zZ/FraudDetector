package dev.thiagogomes.fraud.detector.alert;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record AlertResponse(UUID id, UUID transactionId, String cardId, FraudRule rule, BigDecimal amount,
                            Map<String, Object> details, AlertStatus status, Instant transactionAt, Instant detectedAt) {
    static AlertResponse from(AlertEntity alert) {
        return new AlertResponse(alert.id, alert.transactionId, alert.cardId, alert.rule, alert.amount,
                alert.details, alert.status, alert.transactionAt, alert.detectedAt);
    }
}
