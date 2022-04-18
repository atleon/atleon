package io.atleon.polling.reactive;

import io.atleon.polling.Pollable;
import io.atleon.polling.Polled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

/**
 * The PollingHandler manages the interaction between the event loop which handles polling and the downstream
 * consumers.
 * @param <P> - The type of the objects produced by the poller.
 * @param <O> - The type of the object used for ack and nack.
 */
public interface Poller<P, O> {

    /**
     * A convenience method for creating the default instance of the PollingHandler.
     * @param pollable - The {@link Pollable} implemented by the consumer.
     * @param pollingInterval - The interval that will be used for polling.
     * @param <P> - The type of the objects produced by the poller.
     * @param <O> - The type of the object used for ack and nack.
     * @return - A new PollingHandler instance.
     */
    static <P, O> Poller<P, O> create(final Pollable<P, O> pollable,
                                      final Duration pollingInterval) {
        return new PollerImp<>(pollable, pollingInterval);
    }

    /**
     * A method to start the underlying stream and returns a {@link Flux} from which
     * to consume events.
     * @return - A Flux which represents the stream events from the poller.
     */
    Flux<Collection<Polled<P, O>>> receive();

    /**
     * A method to shut down the polling event loop and stop producing events.
     * @return - A {@link Mono} that completes when the close operations are complete.
     */
    Mono<Void> close();

    /**
     * A method to gain access to the {@link Pollable} supplied for this stream.
     * @return - The {@link Pollable} implementation supplied by the end user.
     */
    Pollable<P, O> getPollable();
}
