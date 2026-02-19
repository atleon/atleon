package io.atleon.util;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Analog of {@link java.util.function.Supplier}, but (potentially) throws {@link IOException}.
 */
public interface IOSupplier<T> {

    /**
     * Wraps/Converts the provided {@link Callable} as/to {@link IOSupplier}.
     */
    static <T> IOSupplier<T> wrap(Callable<T> callable) {
        return () -> Calling.callAsIO(callable);
    }

    T get() throws IOException;
}
