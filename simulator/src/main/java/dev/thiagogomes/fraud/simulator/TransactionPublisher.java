package dev.thiagogomes.fraud.simulator;

import java.util.concurrent.TimeUnit;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class TransactionPublisher {

    static final String TOPIC = "transactions";

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final TransactionGenerator generator;
    private final SimulatorProperties properties;

    TransactionPublisher(KafkaTemplate<String, Transaction> kafkaTemplate,
                         TransactionGenerator generator,
                         SimulatorProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.generator = generator;
        this.properties = properties;
    }

    @Scheduled(fixedRate = 1, timeUnit = TimeUnit.SECONDS)
    void publish() {
        generator.nextBatch(properties.transactionsPerSecond())
                .forEach(transaction -> kafkaTemplate.send(TOPIC, transaction.cardId(), transaction));
    }
}
