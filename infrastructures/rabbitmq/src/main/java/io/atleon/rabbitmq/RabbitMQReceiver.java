package io.atleon.rabbitmq;

import reactor.core.publisher.Flux;

/**
 * A reactive receiver of messages from RabbitMQ, wrapped as {@link RabbitMQReceiverMessage}. An
 * independent {@link com.rabbitmq.client.Connection} and {@link com.rabbitmq.client.Channel} are
 * associated with each receiver subscription, and closed if/when that subscription is canceled or
 * errored out. Each emitted message is associated with a {@link RabbitMQDeliverySettler} which
 * <i>must</i> be invoked to signal the final settlement state of a message. This can be either
 * "ack" (message was successfully fully processed), "reject" (message processing failed fatally,
 * and may be dead-lettered if queue is configured as such), or "requeue" (message processing can
 * or should be retried again in the future).
 * <p>
 * <b>Unsupported Consumption Features:</b> The following are noteworthy unsupported consumption
 * features:
 * <ul>
 * <li><b>Native auto-ack / no-ack:</b> With the underlying reception backed by consumption on a
 * normal RabbitMQ channel, the only mechanism available for controlling back-pressure between a
 * given queue and downstream emission of messages is the native {@code qos} channel setting. This
 * means we are all but forced to use manual acknowledgement in a reactive context.</li>
 * <li><b>No-local:</b> RabbitMQ does not support the {@code noLocal} flag on consumption.</li>
 * <li><b>Exclusive:</b> The {@code exclusive} flag is not supported due to having at least one
 * recommended queue-level alterative ("single active consumer") and not being supported in later
 * versions (i.e. 1.0+) of the AMQP spec.</li>
 * <li><b>Multi-message settlement:</b> Settling more than one message at a time (by setting the
 * "multiple" flag on ack/nack) is dubious to support in a reactive context. In such a context, it
 * is likely (if not assumed) that there is some amount of asynchronous processing occurring, which
 * makes out-of-order completion of messages possible. As such, in order to avoid accidental
 * violation of at-least-once processing semantics, we avoid exposing the "multiple" flag. Note
 * this is commensurate with newer versions (i.e. 1.0+) of the AMQP spec.</li>
 * </ul>
 */
public final class RabbitMQReceiver {

    private final RabbitMQReceiverOptions options;

    private RabbitMQReceiver(RabbitMQReceiverOptions options) {
        this.options = options;
    }

    public static RabbitMQReceiver create(RabbitMQReceiverOptions options) {
        return new RabbitMQReceiver(options);
    }

    /**
     * Receive messages from the given queue with manual settlement control.
     *
     * @param queue The name of the queue to consume from
     * @return A stream of messages that require manual settlement
     */
    public Flux<RabbitMQReceiverMessage> receiveManual(String queue) {
        return receiveManual(new ConsumingSubscriptionFactory(options), queue);
    }

    private Flux<RabbitMQReceiverMessage> receiveManual(
            ConsumingSubscriptionFactory subscriptionFactory, String queue) {
        return Flux.from(it -> it.onSubscribe(subscriptionFactory.create(queue, it)));
    }
}
