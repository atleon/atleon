package io.atleon.core;

import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link DelegatingAlo} that adapts from an {@link Alo} exposing an {@link Optional} that is
 * known to be non-empty to an Alo exposing that Optional's data type.
 *
 * @param <T> The type referenced by the wrapped Alo's Optional
 */
public final class PresentAlo<T> extends AbstractDelegatingAlo<T, Optional<? extends T>> {

    private PresentAlo(Alo<Optional<? extends T>> delegate) {
        super(delegate);
    }

    static <T> PresentAlo<T> wrap(Alo<Optional<? extends T>> delegate) {
        return new PresentAlo<>(delegate);
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return delegate.map(mapper.compose(Optional::get));
    }

    @Override
    public T get() {
        return delegate.get().orElseThrow(PresentAlo::newAbsentDelegateException);
    }

    private static IllegalStateException newAbsentDelegateException() {
        return new IllegalStateException("Something bad happened. Wrapped Alo is absent.");
    }
}
