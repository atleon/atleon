package io.atleon.rabbitmq;

public interface RabbitMQMessageSendInterceptor<T> extends Configurable {

    RabbitMQMessage<T> onSend(RabbitMQMessage<T> rabbitMQMessage);
}
