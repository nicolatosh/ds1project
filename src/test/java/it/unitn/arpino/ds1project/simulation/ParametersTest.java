package it.unitn.arpino.ds1project.simulation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParametersTest {
    @Test
    void instancesShouldBeDifferent() {
        var first = new CoordinatorParameters();
        var second = new CoordinatorParameters();

        second.coordinatorRecoveryTimeMs = 1000;

        Assertions.assertNotEquals(first.coordinatorRecoveryTimeMs, second.coordinatorRecoveryTimeMs);
    }
}
