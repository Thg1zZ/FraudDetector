package dev.thiagogomes.fraud.detector.alert;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "fraud_alert")
class AlertEntity {
    @Id UUID id;
    @Column(nullable = false) UUID transactionId;
    @Column(nullable = false, length = 64) String cardId;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 40) FraudRule rule;
    @Column(nullable = false, precision = 19, scale = 2) BigDecimal amount;
    @JdbcTypeCode(SqlTypes.JSON) @Column(nullable = false, columnDefinition = "jsonb") Map<String, Object> details;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 20) AlertStatus status;
    @Column(nullable = false) Instant transactionAt;
    @Column(nullable = false) Instant detectedAt;

    protected AlertEntity() { }
}
