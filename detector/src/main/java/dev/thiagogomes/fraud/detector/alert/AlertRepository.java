package dev.thiagogomes.fraud.detector.alert;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface AlertRepository extends JpaRepository<AlertEntity, UUID>, JpaSpecificationExecutor<AlertEntity> {
    @Modifying
    @Query(value = """
            INSERT INTO fraud_alert (id, transaction_id, card_id, rule, amount, details, transaction_at, detected_at)
            VALUES (:id, :transactionId, :cardId, :rule, :amount, CAST(:details AS jsonb), :transactionAt, :detectedAt)
            ON CONFLICT DO NOTHING
            """, nativeQuery = true)
    int insert(@Param("id") UUID id, @Param("transactionId") UUID transactionId,
               @Param("cardId") String cardId, @Param("rule") String rule,
               @Param("amount") BigDecimal amount, @Param("details") String details,
               @Param("transactionAt") Instant transactionAt, @Param("detectedAt") Instant detectedAt);
}
