package io.atleon.util;

import java.io.IOException;

/**
 * Analog of {@link Runnable}, but (potentially) throws {@link IOException}.
 */
@FunctionalInterface
public interface IORunnable {
    void run() throws IOException;
}
