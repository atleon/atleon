package io.atleon.rabbitmq;

/**
 * Error indicating that a sent {@link RabbitMQMessage} was not ack'd
 */
public class UnackedRabbitMQMessageException extends RuntimeException {

    UnackedRabbitMQMessageException() {

    }
}
