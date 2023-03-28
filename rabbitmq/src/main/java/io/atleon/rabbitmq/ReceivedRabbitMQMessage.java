package io.atleon.rabbitmq;

import com.rabbitmq.client.AMQP;

public class ReceivedRabbitMQMessage<T> extends RabbitMQMessage<T> {

    private final String queue;

    public ReceivedRabbitMQMessage(String queue, String exchange, String routingKey, AMQP.BasicProperties properties, T body) {
        super(exchange, routingKey, properties, body);
        this.queue = queue;
    }

    public String getQueue() {
        return queue;
    }
}
