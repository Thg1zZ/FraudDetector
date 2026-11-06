package dev.thiagogomes.fraud.simulator;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("simulator")
public record SimulatorProperties(int transactionsPerSecond, int cards, double fraudRate) {
}
