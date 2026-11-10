package dev.thiagogomes.fraud.simulator;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;

class SimulatorPropertiesTest {
    @Test
    void rejectsConfigurationThatCannotGenerateTraffic() {
        assertThatIllegalArgumentException().isThrownBy(() -> new SimulatorProperties(0, 10, 0.1));
        assertThatIllegalArgumentException().isThrownBy(() -> new SimulatorProperties(20, 0, 0.1));
        for (double probability : new double[] {-0.01, 1.01, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatIllegalArgumentException().isThrownBy(() -> new SimulatorProperties(20, 10, probability));
        }
        assertThat(new SimulatorProperties(20, 10, 0).fraudRate()).isZero();
        assertThat(new SimulatorProperties(20, 10, 1).fraudRate()).isEqualTo(1);
    }
}
