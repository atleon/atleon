package io.atleon.rabbitmq;

/**
 * Represents publishing failure due to published delivery being returned by broker.
 */
public class ReturnedOutboundDeliveryException extends OutboundDeliveryException {

    public ReturnedOutboundDeliveryException(int code, String text) {
        super(String.format("Outbound Delivery returned with code=%d and text='%s'", code, text));
    }
}
