package io.atleon.aws.sqs;

import java.time.Duration;

/**
 * Interface through which the visibility of an associated inbound {@link SqsMessage} is managed.
 */
@FunctionalInterface
public interface SqsMessageVisibilityChanger {

    /**
     * Changes the visibility timeout of the associated {@link SqsMessage} by the provided duration
     * and may mark the message as no longer in flight. When stillInFlight is "false", it implies
     * that processing of the Message has terminated and another Message might be requested. When
     * stillInFlight is "true", it implies that the message is still being processed.
     *
     * @param timeout The amount of time to reset the visibility by, by whole seconds
     * @param stillInFlight Whether Message is still being processed
     */
    void execute(Duration timeout, boolean stillInFlight);
}
