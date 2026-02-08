package io.atleon.rabbitmq;

/**
 * Base class for exceptions representing outbound delivery failures.
 */
public class OutboundDeliveryException extends RuntimeException {

    OutboundDeliveryException(String message) {
        super(message);
    }
}
