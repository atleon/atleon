package io.atleon.core;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Partial implementation of {@link DelegatingAlo} that supports both decoration and adaptation.
 *
 * @param <T> The type of data item exposed by this {@link AbstractDelegatingAlo}
 * @param <U> The type of data item actually referenced by this {@link AbstractDelegatingAlo}'s delegate
 */
public abstract class AbstractDelegatingAlo<T, U> implements DelegatingAlo<T> {

    protected final Alo<U> delegate;

    protected AbstractDelegatingAlo(Alo<U> delegate) {
        this.delegate = delegate;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{delegate=" + delegate + "}";
    }

    @Override
    public void runInContext(Runnable runnable) {
        delegate.runInContext(runnable);
    }

    @Override
    public <R> R supplyInContext(Supplier<R> supplier) {
        return delegate.supplyInContext(supplier);
    }

    @Override
    public <R> AloFactory<List<R>> fanInPropagator(List<? extends Alo<?>> alos) {
        return delegate.fanInPropagator(alos);
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return delegate.propagator();
    }

    @Override
    public Runnable getAcknowledger() {
        return delegate.getAcknowledger();
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return delegate.getNacknowledger();
    }

    @Override
    public Alo<U> getDelegate() {
        return delegate;
    }

    protected static <T extends DelegatingAlo<?>> void doOnDelegator(Alo<?> alo, Class<T> type, Consumer<T> consumer) {
        if (type.isInstance(alo)) {
            consumer.accept(type.cast(alo));
        } else if (alo instanceof DelegatingAlo) {
            doOnDelegator(DelegatingAlo.class.cast(alo).getDelegate(), type, consumer);
        }
    }
}
