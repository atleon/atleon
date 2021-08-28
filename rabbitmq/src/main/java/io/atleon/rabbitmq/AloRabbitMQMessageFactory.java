package io.atleon.rabbitmq;

import io.atleon.core.AloFactory;

public interface AloRabbitMQMessageFactory<T> extends AloFactory<RabbitMQMessage<T>>, Configurable {

}
