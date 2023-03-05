package io.atleon.rabbitmq;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.rabbitmq.CorrelableOutboundMessage;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

import java.io.Closeable;
import java.util.function.Function;

/**
 * A reactive RabbitMQ sender with at-least-once semantics for producing messages to exchanges in
 * a RabbitMQ cluster
 * <P>
 * At most one instance of a {@link Sender} is kept and can be closed upon invoking
 * {@link AloRabbitMQSender#close()}. However, if after closing, more sent Publishers are
 * subscribed to, a new Sender instance will be created and cached.
 *
 * @param <T> outbound message body type (to be serialized)
 */
public class AloRabbitMQSender<T> implements Closeable {

    /**
     * Prefix used on all AloRabbitMQSender-specific configurations
     */
    public static final String CONFIG_PREFIX = "rabbitmq.sender.";

    /**
     * An implementation of {@link BodySerializer} used to serialize message bodies
     */
    public static final String BODY_SERIALIZER_CONFIG = CONFIG_PREFIX + "body.serializer";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloRabbitMQSender.class);

    private static final SendOptions SEND_OPTIONS = new SendOptions();

    private static final SendOptions ALO_SEND_OPTIONS = new SendOptions()
        .exceptionHandler(AloRabbitMQSender::handleAloSendException);

    private final Mono<SendResources<T>> futureResources;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private AloRabbitMQSender(RabbitMQConfigSource configSource) {
        this.futureResources = configSource.create()
            .map(SendResources::<T>fromConfig)
            .cacheInvalidateWhen(resources -> closeSink.asFlux().next().then(), SendResources::close);
    }

    /**
     * Creates a new AloRabbitMQSender from the provided {@link RabbitMQConfigSource}
     *
     * @param configSource The reactive source of {@link RabbitMQConfig}
     * @param <T>          The type of messages bodies sent by this sender
     * @return A new AloRabbitMQSender
     */
    public static <T> AloRabbitMQSender<T> from(RabbitMQConfigSource configSource) {
        return new AloRabbitMQSender<>(configSource);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of RabbitMQ message
     * bodies to a Publisher of the results of sending each message body. See
     * {@link #sendBodies(Publisher, RabbitMQMessageCreator)} for further information.
     *
     * @param messageCreator A factory that creates {@link RabbitMQMessage}s from message bodies
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<T>, Flux<RabbitMQSenderResult<T>>> sendBodies(RabbitMQMessageCreator<T> messageCreator) {
        return bodies -> sendBodies(bodies, messageCreator);
    }

    /**
     * Sends a sequence of message bodies to be populated in {@link RabbitMQMessage}s. The
     * destination exchange and routing key (if necessary) must be populated by the provided
     * {@link RabbitMQMessageCreator}.
     * <p>
     * The output of each sent message body is a {@link RabbitMQSenderResult} containing the sent
     * value.
     *
     * @param bodies         A Publisher of RabbitMQ message bodies
     * @param messageCreator A factory that creates {@link RabbitMQMessage}s from message bodies
     * @return a Publisher of the results of each sent message
     */
    public Flux<RabbitMQSenderResult<T>> sendBodies(Publisher<T> bodies, RabbitMQMessageCreator<T> messageCreator) {
        return futureResources.flatMapMany(resources -> resources.send(bodies, messageCreator));
    }

    /**
     * Send a single {@link RabbitMQMessage}
     *
     * @param message A message to send
     * @return A Publisher of the result of sending the message
     */
    public Mono<RabbitMQSenderResult<RabbitMQMessage<T>>> sendMessage(RabbitMQMessage<T> message) {
        return sendMessages(Flux.just(message)).next();
    }

    /**
     * Sends a sequence of {@link RabbitMQMessage}s
     * <p>
     * The output of each sent message is a {@link RabbitMQSenderResult} containing the sent
     * message.
     *
     * @param messages A Publisher of messages to send
     * @return A Publisher of items referencing the result of each sent message
     */
    public Flux<RabbitMQSenderResult<RabbitMQMessage<T>>> sendMessages(Publisher<RabbitMQMessage<T>> messages) {
        return futureResources.flatMapMany(resources -> resources.send(messages, Function.identity()));
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing RabbitMQ message bodies to a Publisher of Alo items referencing the result of
     * sending each message body. See {@link #sendAloBodies(Publisher, RabbitMQMessageCreator)} for
     * further information.
     *
     * @param messageCreator A factory that creates {@link RabbitMQMessage}s from message bodies
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<T>>, AloFlux<RabbitMQSenderResult<T>>> sendAloBodies(
        RabbitMQMessageCreator<T> messageCreator
    ) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing message bodies to be populated in
     * {@link RabbitMQMessage}s. The destination exchange and routing key (if necessary) must be
     * populated by the provided {@link RabbitMQMessageCreator}
     * <p>
     * The output of each sent message body is a {@link RabbitMQSenderResult} containing the sent
     * value. Each emitted item is an {@link Alo} item referencing a {@link RabbitMQSenderResult}
     * and must be acknowledged or nacknowledged such that its processing can be marked complete at
     * the origin of the message.
     *
     * @param aloBodies      A Publisher of Alo items referencing RabbitMQ message bodies
     * @param messageCreator A factory that creates {@link RabbitMQMessage}s from message bodies
     * @return a Publisher of Alo items referencing the result of each sent message
     */
    public AloFlux<RabbitMQSenderResult<T>> sendAloBodies(
        Publisher<Alo<T>> aloBodies,
        RabbitMQMessageCreator<T> messageCreator
    ) {
        return futureResources.flatMapMany(resources -> resources.sendAlos(aloBodies, messageCreator)).as(AloFlux::wrap);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing {@link RabbitMQMessage}s
     * <p>
     * The output of each sent message is a {@link RabbitMQSenderResult} containing the sent
     * message. Each emitted item is an {@link Alo} item referencing a {@link RabbitMQSenderResult}
     * and must be acknowledged or nacknowledged such that its processing can be marked complete at
     * the origin of the message.
     *
     * @param aloMessages A Publisher of Alo items referencing messages to send
     * @return A Publisher of Alo items referencing the result of each sent message
     */
    public AloFlux<RabbitMQSenderResult<RabbitMQMessage<T>>> sendAloMessages(
        Publisher<Alo<RabbitMQMessage<T>>> aloMessages
    ) {
        return futureResources
            .flatMapMany(resources -> resources.sendAlos(aloMessages, Function.identity()))
            .as(AloFlux::wrap);
    }

    /**
     * Closes this sender and logs the provided reason.
     *
     * @param reason The reason this sender is being closed
     */
    public void close(Object reason) {
        LOGGER.info("Closing AloRabbitMQSender due to reason={}", reason);
        close();
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    //TODO This is an ugly result of SendContext not being parameterized on `sendWithTypedPublishConfirms`
    private static void handleAloSendException(Sender.SendContext sendContext, Exception error) {
        CorrelableOutboundMessage message = CorrelableOutboundMessage.class.cast(sendContext.getMessage());
        Alo.nacknowledge(Alo.class.cast(message.getCorrelationMetadata()), error);
    }

    private static final class SendResources<T> {

        private final Sender sender;

        private final BodySerializer<T> bodySerializer;

        public SendResources(Sender sender, BodySerializer<T> bodySerializer) {
            this.sender = sender;
            this.bodySerializer = bodySerializer;
        }

        public static <T> SendResources<T> fromConfig(RabbitMQConfig config) {
            SenderOptions senderOptions = new SenderOptions()
                .connectionFactory(config.getConnectionFactory());

            return new SendResources<>(
                new Sender(senderOptions),
                config.loadConfiguredOrThrow(BODY_SERIALIZER_CONFIG)
            );
        }

        public <R> Flux<RabbitMQSenderResult<R>> send(
            Publisher<R> items,
            Function<R, RabbitMQMessage<T>> messageCreator
        ) {
            return Flux.from(items)
                .map(item -> toCorrelableOutboundMessage(item, messageCreator))
                .transform(outboundMessages -> sender.sendWithTypedPublishConfirms(outboundMessages, SEND_OPTIONS))
                .map(RabbitMQSenderResult::fromMessageResult);
        }

        public <R> Flux<Alo<RabbitMQSenderResult<R>>> sendAlos(
            Publisher<Alo<R>> alos,
            Function<R, RabbitMQMessage<T>> messageCreator
        ) {
            return AloFlux.toFlux(alos)
                .map(alo -> alo.supplyInContext(() -> toCorrelableOutboundMessage(alo, messageCreator.compose(Alo::get))))
                .transform(outboundMessages -> sender.sendWithTypedPublishConfirms(outboundMessages, ALO_SEND_OPTIONS))
                .map(RabbitMQSenderResult::fromMessageResultOfAlo);
        }

        public void close() {
            sender.close();
        }

        private <R> CorrelableOutboundMessage<R> toCorrelableOutboundMessage(
            R data,
            Function<R, RabbitMQMessage<T>> dataToRabbitMQMessage
        ) {
            RabbitMQMessage<T> message = dataToRabbitMQMessage.apply(data);
            return new CorrelableOutboundMessage<>(
                message.getExchange(),
                message.getRoutingKey(),
                message.getProperties(),
                bodySerializer.serialize(message.getBody()).bytes(),
                data
            );
        }
    }
}
