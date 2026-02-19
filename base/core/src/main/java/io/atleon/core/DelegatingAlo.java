package io.atleon.core;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An {@link Alo} that is known to delegate at least some of its functionality to another Alo
 *
 * @param <T> The type of data item exposed by this Alo
 */
public interface DelegatingAlo<T> extends Alo<T> {

    /**
     * {@inheritDoc}
     */
    @Override
    void runInContext(Runnable runnable);

    /**
     * {@inheritDoc}
     */
    @Override
    <R> R supplyInContext(Supplier<R> supplier);

    /**
     * {@inheritDoc}
     */
    @Override
    <R> Alo<R> map(Function<? super T, ? extends R> mapper);

    /**
     * {@inheritDoc}
     */
    @Override
    <R> AloFactory<List<R>> fanInPropagator(List<? extends Alo<?>> alos);

    /**
     * @return The {@link Alo} that this delegates to
     */
    Alo<?> getDelegate();
}
