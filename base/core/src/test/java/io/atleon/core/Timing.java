package io.atleon.core;

import java.util.function.Supplier;

public final class Timing {

    private static final long DEFAULT_WAIT_MILLIS = 10000L;

    private static final long CONDITION_PAUSE_MILLIS = 10L;

    private Timing() {}

    public static void waitForCondition(Supplier<Boolean> condition) {
        waitForCondition(condition, DEFAULT_WAIT_MILLIS);
    }

    public static void waitForCondition(Supplier<Boolean> condition, long maxWaitMillis) {
        long millisWaited = 0L;
        while (millisWaited < maxWaitMillis && !condition.get()) {
            pause(CONDITION_PAUSE_MILLIS);
            millisWaited += CONDITION_PAUSE_MILLIS;
        }
    }

    public static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            System.err.println("Failed to pause: " + e);
        }
    }
}
