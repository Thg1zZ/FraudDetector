package dev.thiagogomes.fraud.detector.alert;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Component
public class AlertConsumer {
    private final AlertRepository repository;
    private final JsonMapper json;

    public AlertConsumer(AlertRepository repository, JsonMapper json) {
        this.repository = repository;
        this.json = json;
    }

    @KafkaListener(topics = "fraud-alerts")
    @Transactional
    public void consume(FraudAlert alert) {
        int inserted = repository.insert(alert.id(), alert.transactionId(), alert.cardId(), alert.rule(), alert.amount(),
                json.writeValueAsString(alert.details()), alert.transactionAt(), alert.detectedAt());
        if (inserted == 0) {
            repository.findById(alert.id()).ifPresent(existing -> {
                if (!existing.transactionId.equals(alert.transactionId()) || !existing.rule.name().equals(alert.rule())) {
                    throw new IllegalArgumentException("ID de alerta já pertence a outra transação ou regra");
                }
            });
        }
    }
}
