package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

/**
 * Container for information that may either be received from a RabbitMQ queue or sent to a
 * RabbitMQ exchange.
 *
 * @param <T> The type of (deserialized) body referenced by this message
 */
public class RabbitMQMessage<T> {

    private final String exchange;

    private final String routingKey;

    private final AMQP.BasicProperties properties;

    private final T body;

    public RabbitMQMessage(String exchange, String routingKey, AMQP.BasicProperties properties, T body) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.body = body;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    public T getBody() {
        return body;
    }
}
