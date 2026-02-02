package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

/**
 * Encapsulates arguments for <code>basicPublish</code> on {@link com.rabbitmq.client.Channel}.
 */
final class BasicPublishArguments {

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final byte[] body;

    public BasicPublishArguments(String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.body = body;
    }

    public <T> OutboundDelivery<T> toOutboundDelivery(T correlationMetadata) {
        return new OutboundDelivery<>(exchange, routingKey, properties, correlationMetadata);
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

    public byte[] body() {
        return body;
    }
}
