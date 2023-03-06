package io.atleon.core;

import java.util.function.Consumer;

/**
 * Factory for creating implementations of {@link Alo}
 *
 * @param <T>
 */
@FunctionalInterface
public interface AloFactory<T> {

    Alo<T> create(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowedger);
}
