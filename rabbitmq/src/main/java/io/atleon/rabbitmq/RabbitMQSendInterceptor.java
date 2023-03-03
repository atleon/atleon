package io.atleon.rabbitmq;

import io.atleon.util.Configurable;

public interface RabbitMQSendInterceptor<T> extends Configurable {

    RabbitMQMessage<T> onSend(RabbitMQMessage<T> rabbitMQMessage);
}
