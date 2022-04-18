package io.atleon.polling;

import io.atleon.polling.reactive.PollingReceiver;

import java.util.Collection;

/**
 * The Pollable class is what manages the interaction with the external resource to be polled. The Poller
 * should be implemented by the user and supplied to {@link PollingReceiver} when instantiated. The supplied
 * implementation will need to have an internal mechanism to track offsets into the external resource as this
 * poll() method has no parameters and is only expecting to receive the next batch of events for the stream.
 * @param <P> The type of the items returned by the external resource.
 * @param <O> The data type of the offsets.
 */
public interface Pollable<P, O> {

    /**
     * This method will be called periodically to fetch new records to push into the stream.
     * @return - A collection of records from the external resource.
     */
    Collection<Polled<P, O>> poll();

    /**
     * This method is used for acknowledging successful processing of an event. This can be used to effective
     * maintain offsets for polling.
     * @param event - The event that is being acknowledged.
     */
    void ack(O event);

    /**
     * This method is used for negatively acknowledging (unsuccessfully processed) an event. Like the ack method above
     * this can be used for effectively tracking offsets.
     * @param event - The even that is being negatively acknowledged.
     */
    void nack(Throwable t, O event);
}
