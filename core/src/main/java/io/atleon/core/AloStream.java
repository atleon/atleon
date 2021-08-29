package io.atleon.core;

import io.atleon.util.Throwing;
import reactor.core.Disposable;

import java.util.concurrent.atomic.AtomicReference;

public abstract class AloStream<C extends AloStreamConfig> {

    public enum State { STOPPED, STARTING, STARTED }

    private static final Disposable EMPTY = () -> {};

    private static final Disposable STARTING = () -> {};

    private final AtomicReference<Disposable> disposableReference = new AtomicReference<>(EMPTY);

    public final synchronized void start(C config) {
        if (!disposableReference.compareAndSet(EMPTY, STARTING) && !disposableReference.get().isDisposed()) {
            throw new UnsupportedOperationException("Cannot start AloStream that is already starting/started");
        }

        try {
            disposableReference.set(startDisposable(config));
        } catch (Throwable error) {
            disposableReference.set(EMPTY);
            throw Throwing.propagate(error);
        }
    }

    public final synchronized void stop() {
        disposableReference.getAndSet(EMPTY).dispose();
    }

    public final State state() {
        Disposable disposable = disposableReference.get();
        if (disposable == STARTING) {
            return State.STARTING;
        } else if (disposable == EMPTY || disposable.isDisposed()) {
            return State.STOPPED;
        } else {
            return State.STARTED;
        }
    }

    protected abstract Disposable startDisposable(C config);
}
