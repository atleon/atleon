package io.atleon.core;

import java.util.Optional;

/**
 * Base interface representing the result of sending a message
 */
public interface SenderResult {

    /**
     * Convenience method for generating error from SenderResult. If the provided SenderResult has
     * a failure cause, that is returned. Else a generic {@link FailureException} wrapping the
     * provided SenderResult is returned.
     *
     * @param senderResult The SenderResult to
     * @return An error that can be thrown or emitted
     */
    static Throwable toError(SenderResult senderResult) {
        return senderResult.failureCause().orElseGet(() -> new FailureException(senderResult));
    }

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

    /**
     * A generic Exception wrapping a SenderResult that has failed to be processed
     */
    class FailureException extends RuntimeException {

        private FailureException(SenderResult senderResult) {
            super("Failed processing where senderResult=" + senderResult);
        }
    }
}
