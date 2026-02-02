package io.atleon.rabbitmq;

/**
 * Represents cancellation of consume operation initiated by broker.
 */
public class CanceledConsumeException extends RuntimeException {

    CanceledConsumeException(String consumerTag) {
        super("Consume canceled with consumerTag=" + consumerTag);
    }
}
