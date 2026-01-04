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

    RabbitMQMessage(String exchange, String routingKey, AMQP.BasicProperties properties, T body) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.properties = properties;
        this.body = body;
    }

    public static <T> RabbitMQMessage<T> create(
            String exchange, String routingKey, AMQP.BasicProperties properties, T body) {
        return new RabbitMQMessage<>(exchange, routingKey, properties, body);
    }

    /**
     * @deprecated Use {@link #exchange()}
     */
    @Deprecated
    public String getExchange() {
        return exchange;
    }

    public String exchange() {
        return exchange;
    }

    /**
     * @deprecated Use {@link #routingKey()}
     */
    @Deprecated
    public String getRoutingKey() {
        return routingKey;
    }

    public String routingKey() {
        return routingKey;
    }

    /**
     * @deprecated Use {@link #properties()}
     */
    @Deprecated
    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    public AMQP.BasicProperties properties() {
        return properties;
    }

    /**
     * @deprecated Use {@link #body()}
     */
    @Deprecated
    public T getBody() {
        return body;
    }

    public T body() {
        return body;
    }
}
