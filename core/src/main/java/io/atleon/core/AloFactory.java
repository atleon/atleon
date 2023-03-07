package io.atleon.core;

import java.util.function.Consumer;

/**
 * Factory for creating implementations of {@link Alo}
 *
 * @param <T>
 */
@FunctionalInterface
public interface AloFactory<T> {

    /**
     * Add decoration to this factory by decorating the Alo's it produces using the provided
     * decorator.
     *
     * @param decorator The {@link AloDecorator} to apply to created Alo's
     * @return A new decorating AloFactory
     */
    default AloFactory<T> withDecorator(AloDecorator<T> decorator) {
        return (t, acknowledger, nacknowledger) -> decorator.decorate(create(t, acknowledger, nacknowledger));
    }

    Alo<T> create(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowedger);
}
