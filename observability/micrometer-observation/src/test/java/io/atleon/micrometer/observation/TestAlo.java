package io.atleon.micrometer.observation;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public final class TestAlo<T> implements Alo<T> {

    private final T data;

    private final AtomicInteger acknowledgerCount = new AtomicInteger();

    private final AtomicInteger nacknowledgerCount = new AtomicInteger();

    public TestAlo(T data) {
        this.data = data;
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        return Alo.super.map(mapper);
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return ComposedAlo.factory();
    }

    @Override
    public T get() {
        return data;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledgerCount::incrementAndGet;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return error -> nacknowledgerCount.incrementAndGet();
    }

    public int acknowledgeCount() {
        return acknowledgerCount.get();
    }

    public int nacknowledgeCount() {
        return nacknowledgerCount.get();
    }
}
