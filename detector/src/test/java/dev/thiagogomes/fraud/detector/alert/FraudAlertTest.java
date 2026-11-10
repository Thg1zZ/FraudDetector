package dev.thiagogomes.fraud.detector.alert;

import static org.assertj.core.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class FraudAlertTest {
    @Test
    void rejectsUnpersistableMoneyAndUnknownRulesAtTheEventBoundary() {
        for (String amount : new String[] {"0", "-0.01", "1.001", "1E+2147483647"}) {
            assertThatIllegalArgumentException().isThrownBy(() -> alert(new BigDecimal(amount), "VELOCITY", Instant.EPOCH));
        }
        assertThatIllegalArgumentException().isThrownBy(() -> alert(BigDecimal.ONE, "UNKNOWN", Instant.EPOCH));
        assertThatIllegalArgumentException().isThrownBy(() -> alert(BigDecimal.ONE, "VELOCITY", Instant.MAX));
        assertThatIllegalArgumentException().isThrownBy(() -> alert(BigDecimal.ONE, "VELOCITY", Instant.EPOCH.minusMillis(1)));
        assertThat(alert(new BigDecimal("99999999999999999.99"), "VELOCITY", Instant.EPOCH).amount())
                .isEqualByComparingTo("99999999999999999.99");
    }

    private FraudAlert alert(BigDecimal amount, String rule, Instant timestamp) {
        return new FraudAlert(new UUID(0, 1), new UUID(0, 2), "card-test", rule, amount, Map.of(), timestamp, timestamp);
    }
}
