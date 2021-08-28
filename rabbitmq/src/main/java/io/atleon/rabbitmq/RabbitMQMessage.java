package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

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
