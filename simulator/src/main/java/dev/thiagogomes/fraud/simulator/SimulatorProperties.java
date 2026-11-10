package dev.thiagogomes.fraud.simulator;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("simulator")
public record SimulatorProperties(int transactionsPerSecond, int cards, double fraudRate) {
    public SimulatorProperties {
        if (transactionsPerSecond < 1 || cards < 1 || !Double.isFinite(fraudRate) || fraudRate < 0 || fraudRate > 1) {
            throw new IllegalArgumentException("Taxa e cartões devem ser positivos; probabilidade de fraude deve estar entre 0 e 1");
        }
    }
}
