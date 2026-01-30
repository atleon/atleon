package io.atleon.util;

import java.io.IOException;

/**
 * Analog of {@link java.util.function.Function}, but (potentially) throws {@link IOException}.
 */
@FunctionalInterface
public interface IOFunction<T, R> {
    R apply(T t) throws IOException;
}
