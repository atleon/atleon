package io.atleon.core;

import java.util.Optional;

/**
 * Base interface representing the result of sending a message
 */
public interface SenderResult {

    /**
     * Whether this result represents a failure to send a message
     *
     * @return True if the result from attempting to send a message has failed
     */
    default boolean isFailure() {
        return failureCause().isPresent();
    }

    /**
     * If this result represents a failure to send a message, indicates an underlying cause, if
     * available
     *
     * @return Underlying cause of send failure if available
     */
    Optional<Throwable> failureCause();
}
