package io.atleon.core;

import java.util.function.Function;

final class OrderManagingAcknowledgementQueue extends AcknowledgementQueue {

    private OrderManagingAcknowledgementQueue() {

    }

    public static AcknowledgementQueue create() {
        return new OrderManagingAcknowledgementQueue();
    }

    @Override
    protected boolean complete(InFlight inFlight, Function<InFlight, Boolean> completer) {
        return completer.apply(inFlight);
    }
}
