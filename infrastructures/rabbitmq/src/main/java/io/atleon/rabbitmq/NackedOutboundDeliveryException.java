package io.atleon.rabbitmq;

/**
 * Represents publishing failure due to delivery being negatively acknowledged by broker.
 */
public class NackedOutboundDeliveryException extends OutboundDeliveryException {

    NackedOutboundDeliveryException(long deliveryTag, boolean multiple) {
        super(String.format("Delivery with tag=%d was nacked with multiple=%b", deliveryTag, multiple));
    }
}
