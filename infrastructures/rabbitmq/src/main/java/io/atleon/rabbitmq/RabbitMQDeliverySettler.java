package io.atleon.rabbitmq;

/**
 * Interface through which settlement of RabbitMQ message delivery is (scheduled to be) performed.
 */
public interface RabbitMQDeliverySettler {

    /**
     * Settle with status used to indicate "successful processing".
     */
    void ack();

    /**
     * Settle with status used to indicate "permanent/fatally failed processing". May trigger
     * dead-lettering if queue is configured as such.
     */
    void reject();

    /**
     * Settle with status used to indicate "temporary/retriable processing failure". Message should
     * be re-queued on the originating queue.
     */
    void requeue();
}
