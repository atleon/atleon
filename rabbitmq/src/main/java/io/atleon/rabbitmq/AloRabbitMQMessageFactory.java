package io.atleon.rabbitmq;

import io.atleon.core.AloFactory;
import io.atleon.util.Configurable;

public interface AloRabbitMQMessageFactory<T> extends AloFactory<RabbitMQMessage<T>>, Configurable {

}
