package io.atleon.rabbitmq;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * A reactive sender for RabbitMQ messages, wrapped as {@link RabbitMQSenderMessage}. This class
 * provides a high-level API for publishing messages to RabbitMQ exchanges in a non-blocking,
 * asynchronous, and backpressure-aware manner using Reactor.
 * <p>
 * Each instance of {@link RabbitMQSender} manages its own RabbitMQ
 * {@link com.rabbitmq.client.Connection}. Messages can be sent with or without custom correlation
 * metadata, and results are emitted as {@link RabbitMQSenderResult} objects, which provide access
 * to RabbitMQ metadata and any error that occurred during sending.
 * <p>
 * <b>Unsupported Publishing Features:</b> The following are noteworthy unsupported publishing
 * features:
 * <ul>
 * <li><b>Mandatory:</b> The mandatory flag is not supported due to inherent limitations on how
 * RabbitMQ implements AMQP. Usage of this flag implies usage of "return listening", which is
 * decoupled from the concept of confirmation (ack and nack). Ack/Nack corresponds with the broker
 * saying "I accept this message", whereas "return" means a mandatory message could not be routed,
 * regardless of whether the broker "accepted" the message. Crucially, nothing in RabbitMQ
 * guarantees that an unrouted mandatory message will be nack'd (rather than ack'd). In short, it
 * is not possible to guarantee that a "mandatory" publish that ultimately cannot be routed will
 * not be nack'd or (more importantly) ack'd prior to invocation of the return listener. In the
 * case of a preceding nack, we cannot identify that such a result is due to not being routable, so
 * any error could be misleading. In the case of an ack, we cannot know that a "return" could be
 * later signaled, which means we have no real-time way of knowing with certainty that a mandatory
 * message has been successfully routed.</li>
 * <li><b>Immediate:</b> RabbitMQ explicitly does not support the immediate flag on publish
 * operations.</li>
 * </ul>
 */
public final class RabbitMQSender {

    private final RabbitMQSenderOptions options;

    private final Mono<SendingConnection> futureConnection;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private RabbitMQSender(RabbitMQSenderOptions options) {
        this.options = options;
        this.futureConnection = SendingConnection.create(options)
                .cacheInvalidateWhen(
                        it -> closeSink.asFlux().next().then().or(it.closed()), SendingConnection::closeSafelyAsync);
    }

    public static RabbitMQSender create(RabbitMQSenderOptions options) {
        return new RabbitMQSender(options);
    }

    /**
     * Sends a single {@link RabbitMQSenderMessage} to RabbitMQ. The resulting Mono will either
     * emit an error if the send operation fails, or a {@link RabbitMQSenderResult} if the send
     * operation succeeds.
     *
     * @param senderMessage The message to send
     * @param <T>           The type of correlation metadata
     * @return a {@link Mono} emitting the result of the send operation
     */
    public <T> Mono<RabbitMQSenderResult<T>> send(RabbitMQSenderMessage<T> senderMessage) {
        return Mono.just(senderMessage).as(this::send).single();
    }

    /**
     * Sends a stream of {@link RabbitMQSenderMessage}s to RabbitMQ and emits results for each
     * record as they are acknowledged via the underlying connection/channel(s). Errors are
     * propagated immediately and terminate the stream.
     *
     * @param senderMessages The publisher of messages to send
     * @param <T>            The type of correlation metadata
     * @return a {@link Flux} emitting (successful) results for each sent message
     */
    public <T> Flux<RabbitMQSenderResult<T>> send(Publisher<RabbitMQSenderMessage<T>> senderMessages) {
        return futureConnection.flatMapMany(it -> send(it, senderMessages));
    }

    /**
     * Sends a stream of {@link RabbitMQSenderMessage}s to RabbitMQ and emits results for each
     * record as they are acknowledged via the underlying connection/channel(s). Errors (either due
     * to negative acknowledgement, IO errors, or returned messages) are delegated to the client
     * for handling by encapsulating both send failures and successes in emitted results.
     *
     * @param senderMessages The publisher of messages to send
     * @param <T>            The type of correlation metadata
     * @return a {@link Flux} emitting results for each sent message, with errors delegated per message
     */
    public <T> Flux<RabbitMQSenderResult<T>> sendDelegateError(Publisher<RabbitMQSenderMessage<T>> senderMessages) {
        return futureConnection.flatMapMany(it -> sendDelegateError(it, senderMessages));
    }

    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private <T> Flux<RabbitMQSenderResult<T>> send(
            SendingConnection connection, Publisher<RabbitMQSenderMessage<T>> senderMessages) {
        return connection.useChannel(it -> SendPublisher.immediateError(options, it, senderMessages));
    }

    private <T> Flux<RabbitMQSenderResult<T>> sendDelegateError(
            SendingConnection connection, Publisher<RabbitMQSenderMessage<T>> senderMessages) {
        return connection.useChannel(it -> SendPublisher.delegateError(options, it, senderMessages));
    }
}
