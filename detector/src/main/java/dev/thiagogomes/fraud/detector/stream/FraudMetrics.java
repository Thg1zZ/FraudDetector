package dev.thiagogomes.fraud.detector.stream;

import java.time.Duration;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import dev.thiagogomes.fraud.detector.alert.FraudAlert;

@Component
public class FraudMetrics {
    private final MeterRegistry registry;
    private final Counter transactions;
    private final Timer latency;

    public FraudMetrics(MeterRegistry registry) {
        this.registry = registry;
        transactions = registry.counter("fraud.transactions");
        latency = Timer.builder("fraud.detection.latency").publishPercentileHistogram()
                .serviceLevelObjectives(Duration.ofMillis(10), Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(30))
                .register(registry);
    }

    public void transactionProcessed() { transactions.increment(); }

    public void alertDetected(FraudAlert alert) {
        registry.counter("fraud.alerts", "rule", alert.rule()).increment();
        Duration delay = Duration.between(alert.transactionAt(), alert.detectedAt());
        latency.record(delay.isNegative() ? Duration.ZERO : delay);
    }
}
