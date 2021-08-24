package io.atleon.core;

import java.util.function.Consumer;

/**
 * The most simple implementation of {@link Alo}. This implementation is fully composed of its
 * data item, acknowledger, and nacknowledger. Propagation includes no extra information.
 *
 * @param <T> The type data item
 */
public class ComposedAlo<T> extends AbstractAlo<T> {

    private final T t;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public ComposedAlo(T t, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        this.t = t;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }


    @Override
    protected <R> AloFactory<R> createPropagator() {
        return ComposedAlo::new;
    }

    @Override
    public T get() {
        return t;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }
}
