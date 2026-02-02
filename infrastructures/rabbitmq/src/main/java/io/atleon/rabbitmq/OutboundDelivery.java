package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

/**
 * A delivery for which publishing has been (or is about to be) attempted.
 */
final class OutboundDelivery<T> {

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final T correlationMetadata;

    public OutboundDelivery(
            String exchange, String routingKey, AMQP.BasicProperties properties, T correlationMetadata) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.correlationMetadata = correlationMetadata;
    }

    public String exchange() {
        return exchange;
    }

    public String routingKey() {
        return routingKey;
    }

    public AMQP.BasicProperties properties() {
        return properties;
    }

    public T correlationMetadata() {
        return correlationMetadata;
    }
}
