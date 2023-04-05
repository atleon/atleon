package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

public class ReceivedRabbitMQMessage<T> extends RabbitMQMessage<T> {

    public ReceivedRabbitMQMessage(
        String exchange,
        String routingKey,
        AMQP.BasicProperties properties,
        T body
    ) {
        super(exchange, routingKey, properties, body);
    }
}
