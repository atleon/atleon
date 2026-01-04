package io.atleon.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ResubscriptionConfigTest {

    @Test
    public void resubscriptionIsEnabledIfDelayIsNonNegative() {
        assertTrue(new ResubscriptionConfig("name", Duration.ofSeconds(1L)).isEnabled());
        assertTrue(new ResubscriptionConfig("name", Duration.ofSeconds(0L)).isEnabled());
        assertFalse(new ResubscriptionConfig("name", Duration.ofSeconds(-1L)).isEnabled());
        assertFalse(new ResubscriptionConfig("name", Duration.parse("PT-1S")).isEnabled());
    }
}
