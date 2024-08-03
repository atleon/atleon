package io.atleon.core;

import io.atleon.util.Throwing;
import org.jetbrains.annotations.NotNull;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A managed Alo streaming process that can be started and stopped
 *
 * @param <C> The type of Config used to start this reactive streaming process
 */
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

    @NotNull
    protected abstract Disposable startDisposable(@NotNull C config);

    protected static Scheduler newBoundedElasticScheduler(String name, int threadCap) {
        return Schedulers.newBoundedElastic(threadCap, Integer.MAX_VALUE, name);
    }
}
