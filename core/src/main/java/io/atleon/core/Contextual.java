package io.atleon.core;

import java.util.function.Supplier;

/**
 * An object that may have some context associated with it.
 */
public interface Contextual {

    /**
     * Invoke the given {@link Runnable} while managing context around its invocation.
     *
     * @param runnable The {@link Runnable} to invoke
     */
    default void runInContext(Runnable runnable) {
        runnable.run();
    }

    /**
     * Invoke the given {@link Supplier} while managing context around its invocation.
     *
     * @param supplier A {@link Supplier} to invoke
     * @param <R>      The type of result produced by the {@link Supplier}
     * @return The value resulting from invoking the {@link Supplier}
     */
    default <R> R supplyInContext(Supplier<R> supplier) {
        return supplier.get();
    }
}
