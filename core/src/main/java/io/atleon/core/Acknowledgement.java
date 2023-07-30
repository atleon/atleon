package io.atleon.core;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Wrapper for positive and negative acknowledgement that guarantees only one is ever executed.
 */
public final class Acknowledgement {

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    private final AtomicBoolean once = new AtomicBoolean(false);

    private Acknowledgement(Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    public static Acknowledgement create(Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        return new Acknowledgement(acknowledger, nacknowledger);
    }

    public void positive() {
        if (once.compareAndSet(false, true)) {
            acknowledger.run();
        }
    }

    public void negative(Throwable error) {
        if (once.compareAndSet(false, true)) {
            nacknowledger.accept(error);
        }
    }
}
