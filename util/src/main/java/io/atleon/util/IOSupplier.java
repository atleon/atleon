package io.atleon.util;

import java.io.IOException;

/**
 * Analog of {@link java.util.function.Supplier}, but (potentially) throws {@link IOException}.
 */
public interface IOSupplier<T> {

    T get() throws IOException;
}
