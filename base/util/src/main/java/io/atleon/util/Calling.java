package io.atleon.util;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Utility methods for working with {@link Callable}.
 */
public final class Calling {

    private Calling() {}

    /**
     * Immediately invokes the provided {@link Callable}, wrapping uncaught checked exceptions as
     * {@link IOException}.
     */
    public static <T> T callAsIO(Callable<T> callable) throws IOException {
        try {
            return callable.call();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw RuntimeException.class.cast(e);
            } else {
                throw Throwing.propagateIO(e);
            }
        }
    }
}
