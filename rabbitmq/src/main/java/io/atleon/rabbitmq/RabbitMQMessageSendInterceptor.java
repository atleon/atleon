package io.atleon.rabbitmq;

import io.atleon.util.Configurable;

public interface RabbitMQMessageSendInterceptor<T> extends Configurable {

    RabbitMQMessage<T> onSend(RabbitMQMessage<T> rabbitMQMessage, SerializedBody serializedBody);
}
