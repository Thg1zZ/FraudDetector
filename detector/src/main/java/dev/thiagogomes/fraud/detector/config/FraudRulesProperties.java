package dev.thiagogomes.fraud.detector.config;

import java.time.Duration;
import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("fraud.rules")
public record FraudRulesProperties(Velocity velocity, HighAmount highAmount,
                                   ImpossibleTravel impossibleTravel, CardTesting cardTesting) {

    public FraudRulesProperties {
        if (velocity == null || highAmount == null || impossibleTravel == null || cardTesting == null) {
            throw new IllegalArgumentException("Todas as regras devem ser configuradas");
        }
    }

    public record HighAmount(BigDecimal multiplier, int minHistory) {
        public HighAmount {
            if (multiplier == null || multiplier.signum() <= 0 || minHistory < 1) {
                throw new IllegalArgumentException("multiplier e min-history devem ser positivos");
            }
        }
    }

    public record ImpossibleTravel(double maxSpeedKmh) {
        public ImpossibleTravel {
            if (!Double.isFinite(maxSpeedKmh) || maxSpeedKmh <= 0) {
                throw new IllegalArgumentException("max-speed-kmh deve ser positivo e finito");
            }
        }
    }

    public record CardTesting(BigDecimal maxAmount, int minTransactions, Duration window, Duration grace) {
        public CardTesting {
            if (maxAmount == null || maxAmount.signum() <= 0 || minTransactions < 1) {
                throw new IllegalArgumentException("max-amount e min-transactions devem ser positivos");
            }
            if (window == null || window.toMillis() < 1 || grace == null || grace.isNegative()) {
                throw new IllegalArgumentException("window deve ser positivo e grace não negativo");
            }
        }
    }

    public record Velocity(int maxTransactions, Duration window, Duration grace) {

        public Velocity {
            if (maxTransactions < 1) {
                throw new IllegalArgumentException("max-transactions deve ser positivo");
            }
            if (window == null || window.toMillis() < 1) {
                throw new IllegalArgumentException("window deve ser de pelo menos 1 ms");
            }
            if (grace == null || grace.isNegative()) {
                throw new IllegalArgumentException("grace não pode ser negativo");
            }
        }
    }
}
