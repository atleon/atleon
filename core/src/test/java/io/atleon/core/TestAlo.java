package io.atleon.core;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class TestAlo extends AbstractAlo<String> {

    private final String data;

    private final Runnable acknowledgerHook;

    private final AtomicBoolean acknowledged = new AtomicBoolean(false);

    private final AtomicReference<Throwable> nacknowledged = new AtomicReference<>();

    public TestAlo(String data) {
        this(data, () -> {});
    }

    public TestAlo(String data, Runnable acknowledgerHook) {
        this.data = data;
        this.acknowledgerHook = acknowledgerHook;
    }

    @Override
    public String get() {
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

    public int length() {
        return data.length();
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

    @Override
    protected <R> AloFactory<R> createPropagator() {
        return ComposedAlo::new;
    }
}
