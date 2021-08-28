package io.atleon.rabbitmq;

import java.util.function.Function;

public interface RabbitMQMessageCreator<T> extends Function<T, RabbitMQMessage<T>> {

}
