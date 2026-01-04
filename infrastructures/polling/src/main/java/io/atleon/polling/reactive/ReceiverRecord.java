package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import io.atleon.polling.Polled;

public class ReceiverRecord<P, O> {

    private final Polled<P, O> record;
    private final Pollable<P, O> pollable;

    public ReceiverRecord(final Polled<P, O> record, final Pollable<P, O> pollable) {
        this.record = record;
        this.pollable = pollable;
    }

    public Polled<P, O> getRecord() {
        return record;
    }

    public Pollable<P, O> getPollable() {
        return pollable;
    }
}
