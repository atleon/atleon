package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import reactor.core.publisher.Flux;

/**
 * The main entry point into the Polling backed stream.
 * @param <P> The type of the objects in the stream.
 * @param <O> The type of the object used for acks and nacks.
 */
public interface PollingReceiver<P, O> {

    /**
     * A helper method to create a PollingReceiver.
     * @param pollable - The user's implementation of the {@link Pollable} interface.
     * @param pollerOptions - Options passed to the polling mechanism.
     * @param <P> The type of the objects in the stream.
     * @param <O> The type of the offsets.
     * @return - A PollingReceived instance.
     */
    static <P, O> PollingReceiverImp<P, O> create(final Pollable<P, O> pollable,
                                                  final PollerOptions pollerOptions) {
        return new PollingReceiverImp<>(pollable, pollerOptions);
    }

    /**
     * A method to start the polling mechanism producing events into the stream.
     * @return A {@link Flux} representing the stream.
     */
    Flux<ReceiverRecord<P, O>> receive();

}
