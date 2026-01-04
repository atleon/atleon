package io.atleon.opentracing;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.ComposedAlo;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public final class TestAlo implements Alo<String> {

    private final String data;

    private final AtomicInteger acknowledgerCount = new AtomicInteger();

    private final AtomicInteger nacknowledgerCount = new AtomicInteger();

    public TestAlo(String data) {
        this.data = data;
    }

    @Override
    public <R> Alo<R> map(Function<? super String, ? extends R> mapper) {
        return Alo.super.map(mapper);
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return ComposedAlo.factory();
    }

    @Override
    public String get() {
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

    public boolean isAcknowledged() {
        return acknowledgerCount.get() > 0;
    }

    public int acknowledgeCount() {
        return acknowledgerCount.get();
    }

    public boolean isNacknowledged() {
        return nacknowledgerCount.get() > 0;
    }

    public int nacknowledgeCount() {
        return nacknowledgerCount.get();
    }
}
