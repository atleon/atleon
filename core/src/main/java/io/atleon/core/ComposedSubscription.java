package io.atleon.core;

import org.reactivestreams.Subscription;

import java.util.function.Consumer;

final class ComposedSubscription implements Subscription {

    private final Consumer<? super Long> onRequest;

    private final Runnable onCancel;

    public ComposedSubscription(Consumer<? super Long> onRequest, Runnable onCancel) {
        this.onRequest = onRequest;
        this.onCancel = onCancel;
    }

    @Override
    public void request(long n) {
        onRequest.accept(n);
    }

    @Override
    public void cancel() {
        onCancel.run();
    }
}
