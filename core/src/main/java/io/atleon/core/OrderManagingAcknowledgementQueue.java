package io.atleon.core;

import java.util.function.Function;

final class OrderManagingAcknowledgementQueue extends AcknowledgementQueue {

    private OrderManagingAcknowledgementQueue(boolean executeErrorsImmediately) {
        super(executeErrorsImmediately);
    }

    public static OrderManagingAcknowledgementQueue newWithImmediateErrors() {
        return new OrderManagingAcknowledgementQueue(true);
    }

    public static OrderManagingAcknowledgementQueue newWithInOrderErrors() {
        return new OrderManagingAcknowledgementQueue(false);
    }

    @Override
    protected boolean complete(InFlight inFlight, Function<InFlight, Boolean> completer) {
        return completer.apply(inFlight);
    }
}
