package dev.thiagogomes.fraud.detector.transaction;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TransactionTest {
    @Test
    void acceptsTheDatabaseMoneyBoundaryAndScientificNotationWithoutRounding() {
        for (String amount : new String[] {"0.01", "99999999999999999.99", "1E+16"}) {
            assertThat(new Transaction(new UUID(0, 1), "c".repeat(64), new BigDecimal(amount),
                    "Loja", "Cidade", -90, 180, Instant.EPOCH).amount()).isEqualByComparingTo(amount);
        }
    }
    @Test
    void rejectsAmountsThatCannotBePersistedWithoutLoss() {
        for (String amount : new String[] {"0", "-1", "1.001", "100000000000000000", "1E+2147483647"}) {
            assertThatIllegalArgumentException().isThrownBy(() -> new Transaction(new UUID(0, 1), "card-0042",
                    new BigDecimal(amount), "Loja", "Cidade", 0, 0, Instant.EPOCH));
        }
    }

    @Test
    void rejectsEventTimeThatCannotBeConvertedToKafkaMilliseconds() {
        assertThatIllegalArgumentException().isThrownBy(() -> new Transaction(new UUID(0, 1), "card-0042",
                BigDecimal.ONE, "Loja", "Cidade", 0, 0, Instant.MAX));
    }

    @Test
    void rejectsInvalidCoordinatesAndMissingEventTime() {
        for (double latitude : new double[] {Double.NaN, Double.POSITIVE_INFINITY, 90.1, -90.1}) {
            assertThatIllegalArgumentException().isThrownBy(() -> new Transaction(new UUID(0, 1), "card-0042",
                    BigDecimal.ONE, "Loja", "Cidade", latitude, 0, Instant.EPOCH));
        }
        assertThatIllegalArgumentException().isThrownBy(() -> new Transaction(new UUID(0, 1), "card-0042",
                BigDecimal.ONE, "Loja", "Cidade", 0, 0, null));
    }
}
