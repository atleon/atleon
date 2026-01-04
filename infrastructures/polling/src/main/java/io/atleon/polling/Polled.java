package io.atleon.polling;

import java.util.Optional;

/**
 * The type of the records that are produced by the {@link Pollable}.
 * @param <P> The type of the payload of the events.
 * @param <O> The type of the object used for determining offsets.
 */
public interface Polled<P, O> {

    static <P, O> Polled<P, O> compose(final P payload, final O offset) {
        return new ComposedPolled<>(payload, offset);
    }

    /**
     * Method to get the payload of the Polled record.
     * @return A payload of type P
     */
    P getPayload();

    /**
     * Method to get the data necessary to commit offsets.
     * @return An object used for determining offsets.
     */
    O getOffset();

    /**
     * Get the function to extract the grouping of an event to ensure order within a group.
     * @return An object that represents the group of the record.
     */
    Optional<?> getGroup();
}
