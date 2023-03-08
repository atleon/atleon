package io.atleon.core;

import java.util.function.Function;

/**
 * Base implementation for a {@link DelegatingAlo} that decorates the delegation of invocations
 * to another {@link Alo}.
 *
 * @param <T> The type of data item exposed by the decorated {@link Alo}
 */
public abstract class AbstractDecoratingAlo<T> extends AbstractDelegatingAlo<T, T> {

    protected AbstractDecoratingAlo(Alo<T> delegate) {
        super(delegate);
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return delegate.map(mapper);
    }

    @Override
    public T get() {
        return delegate.get();
    }
}
