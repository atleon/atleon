package io.atleon.core;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

public class GenericAlo<T> implements Alo<T> {

    private final AtomicInteger mapCount = new AtomicInteger();

    private final T data;

    private final Runnable acknowledgerHook;

    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

    private final AtomicReference<Throwable> nacknowledged = new AtomicReference<>();

    public GenericAlo(T data) {
        this(data, () -> {});
    }

    public GenericAlo(T data, Runnable acknowledgerHook) {
        this.data = data;
        this.acknowledgerHook = acknowledgerHook;
    }

    @Override
    public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
        mapCount.incrementAndGet();
        return Alo.super.map(mapper);
    }

    @Override
    public <R> AloFactory<R> propagator() {
        return Propagated.factory(mapCount);
    }

    @Override
    public T get() {
        return data;
    }

    @Override
    public Runnable getAcknowledger() {
        return () -> {
            acknowledgerHook.run();
            acknowledged.set(true);
        };
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledged::set;
    }

    public int mapCount() {
        return mapCount.get();
    }

    public boolean isAcknowledged() {
        return acknowledged.get();
    }

    public boolean isNacknowledged() {
        return getError().isPresent();
    }

    public Optional<Throwable> getError() {
        return Optional.ofNullable(nacknowledged.get());
    }

    private static final class Propagated<T> implements Alo<T> {

        private final AtomicInteger mapCount;

        private final T data;

        private final Runnable acknowledger;

        private final Consumer<? super Throwable> nacknowledger;

        private Propagated(
            AtomicInteger mapCount,
            T data,
            Runnable acknowledger,
            Consumer<? super Throwable> nacknowledger
        ) {
            this.mapCount = mapCount;
            this.data = data;
            this.acknowledger = acknowledger;
            this.nacknowledger = nacknowledger;
        }

        public static <R> AloFactory<R> factory(AtomicInteger mapCount) {
            return (data, acknowledger, nacknowledger) -> new Propagated<>(mapCount, data, acknowledger, nacknowledger);
        }

        @Override
        public <R> Alo<R> map(Function<? super T, ? extends R> mapper) {
            mapCount.incrementAndGet();
            return Alo.super.map(mapper);
        }

        @Override
        public <R> AloFactory<R> propagator() {
            return factory(mapCount);
        }

        @Override
        public T get() {
            return data;
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
}
