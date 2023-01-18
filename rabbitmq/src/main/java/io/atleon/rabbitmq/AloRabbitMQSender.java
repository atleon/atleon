package io.atleon.rabbitmq;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.CorrelableOutboundMessage;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

import java.util.List;
import java.util.function.Function;

/**
 * A reactive RabbitMQ sender with at-least-once semantics for producing messages to exchanges in
 * a RabbitMQ cluster
 *
 * @param <T> outbound message body type (to be serialized)
 */
public class AloRabbitMQSender<T> {

    /**
     * Prefix used on all AloRabbitMQSender-specific configurations
     */
    public static final String CONFIG_PREFIX = "rabbitmq.sender.";

    /**
     * Optional List (comma separated or {@link List}) of implementations of
     * {@link RabbitMQMessageSendInterceptor} (by class name) to apply to outbound
     * {@link RabbitMQMessage}s
     */
    public static final String INTERCEPTORS_CONFIG = CONFIG_PREFIX + "send.interceptors";

    /**
     * An implementation of {@link BodySerializer} used to serialize message bodies
     */
    public static final String BODY_SERIALIZER_CONFIG = CONFIG_PREFIX + "body.serializer";

    private static final SendOptions SEND_OPTIONS = new SendOptions();

    private static final SendOptions ALO_SEND_OPTIONS = new SendOptions()
        .exceptionHandler(AloRabbitMQSender::handleAloSendException);

    private final Mono<SendResources<T>> futureResources;

    private AloRabbitMQSender(RabbitMQConfigSource configSource) {
        this.futureResources = configSource.create()
            .map(SendResources::<T>fromConfig)
            .cache();
    }

    public static <T> AloRabbitMQSender<T> from(RabbitMQConfigSource configSource) {
        return new AloRabbitMQSender<>(configSource);
    }

    public Function<Publisher<T>, Flux<RabbitMQSenderResult<T>>> sendBodies(RabbitMQMessageCreator<T> messageCreator) {
        return bodies -> sendBodies(bodies, messageCreator);
    }

    public Flux<RabbitMQSenderResult<T>> sendBodies(Publisher<T> bodies, RabbitMQMessageCreator<T> messageCreator) {
        return futureResources
            .flatMapMany(resources -> resources.send(bodies, messageCreator));
    }

    public Flux<RabbitMQSenderResult<RabbitMQMessage<T>>> sendMessages(Publisher<RabbitMQMessage<T>> messages) {
        return futureResources
            .flatMapMany(resources -> resources.send(messages, Function.identity()));
    }

    public Function<Publisher<Alo<T>>, AloFlux<RabbitMQSenderResult<T>>> sendAloBodies(
        RabbitMQMessageCreator<T> messageCreator
    ) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator);
    }

    public AloFlux<RabbitMQSenderResult<T>> sendAloBodies(
        Publisher<Alo<T>> aloBodies,
        RabbitMQMessageCreator<T> messageCreator
    ) {
        return futureResources
            .flatMapMany(resources -> resources.sendAlos(aloBodies, messageCreator))
            .as(AloFlux::wrap);
    }

    public AloFlux<RabbitMQSenderResult<RabbitMQMessage<T>>> sendAloMessages(
        Publisher<Alo<RabbitMQMessage<T>>> aloMessages
    ) {
        return futureResources
            .flatMapMany(resources -> resources.sendAlos(aloMessages, Function.identity()))
            .as(AloFlux::wrap);
    }

    //TODO This is an ugly result of SendContext not being parameterized on `sendWithTypedPublishConfirms`
    private static void handleAloSendException(Sender.SendContext sendContext, Exception error) {
        CorrelableOutboundMessage message = CorrelableOutboundMessage.class.cast(sendContext.getMessage());
        Alo.nacknowledge(Alo.class.cast(message.getCorrelationMetadata()), error);
    }

    private static final class SendResources<T> {

        private final Sender sender;

        private final List<RabbitMQMessageSendInterceptor<T>> interceptors;

        private final BodySerializer<T> bodySerializer;

        public SendResources(
            Sender sender,
            List<RabbitMQMessageSendInterceptor<T>> interceptors,
            BodySerializer<T> bodySerializer) {
            this.sender = sender;
            this.bodySerializer = bodySerializer;
            this.interceptors = interceptors;
        }

        public static <T> SendResources<T> fromConfig(RabbitMQConfig config) {
            SenderOptions senderOptions = new SenderOptions()
                .connectionFactory(config.getConnectionFactory());

            return new SendResources<>(
                new Sender(senderOptions),
                config.loadListOfConfigured(INTERCEPTORS_CONFIG),
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
            Publisher<Alo<R>> aloItems,
            Function<R, RabbitMQMessage<T>> messageCreator
        ) {
            return AloFlux.toFlux(aloItems)
                .map(aloItem -> toCorrelableOutboundMessage(aloItem, messageCreator.compose(Alo::get)))
                .transform(outboundMessages -> sender.sendWithTypedPublishConfirms(outboundMessages, ALO_SEND_OPTIONS))
                .map(RabbitMQSenderResult::fromMessageResultOfAlo);
        }

        private <R> CorrelableOutboundMessage<R> toCorrelableOutboundMessage(
            R data,
            Function<R, RabbitMQMessage<T>> dataToRabbitMQMessage
        ) {
            RabbitMQMessage<T> message = dataToRabbitMQMessage.apply(data);
            SerializedBody serializedBody = bodySerializer.serialize(message.getBody());
            for (RabbitMQMessageSendInterceptor<T> interceptor : interceptors) {
                message = interceptor.onSend(message, serializedBody);
            }
            return new CorrelableOutboundMessage<>(
                message.getExchange(),
                message.getRoutingKey(),
                message.getProperties(),
                serializedBody.bytes(),
                data
            );
        }
    }
}
