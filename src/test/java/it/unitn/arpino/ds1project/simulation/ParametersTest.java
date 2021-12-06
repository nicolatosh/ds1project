package it.unitn.arpino.ds1project.simulation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ParametersTest {
    @Test
    void instancesShouldBeDifferent() {
        var first = new Parameters();
        var second = new Parameters();

        second.coordinatorRecoveryTimeMs = 1000;

        Assertions.assertNotEquals(first.coordinatorRecoveryTimeMs, second.coordinatorRecoveryTimeMs);
    }
}
