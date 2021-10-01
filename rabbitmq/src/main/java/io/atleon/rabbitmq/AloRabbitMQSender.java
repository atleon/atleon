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

public class AloRabbitMQSender<T> {

    /**
     * Prefix used on all AloRabbitMQSender-specific configurations
     */
    public static final String CONFIG_PREFIX = "rabbitmq-sender-";

    /**
     * Optional List (comma separated or {@link List}) of implementations of
     * {@link RabbitMQMessageSendInterceptor} (by class name) to apply to outbound
     * {@link RabbitMQMessage}s
     */
    public static final String INTERCEPTORS_CONFIG = CONFIG_PREFIX + "send-interceptors";

    /**
     * An implementation of {@link BodySerializer} used to serialize message bodies
     */
    public static final String BODY_SERIALIZER_CONFIG = CONFIG_PREFIX + "body-serializer";

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

    public Function<Publisher<T>, Flux<RabbitMQSenderResult<T>>>
    sendBodies(RabbitMQMessageCreator<T> messageCreator) {
        return bodies -> sendBodies(bodies, messageCreator);
    }

    public Flux<RabbitMQSenderResult<T>>
    sendBodies(Publisher<T> bodies, RabbitMQMessageCreator<T> messageCreator) {
        return futureResources
            .flatMapMany(resources -> sendBodies(resources, bodies, messageCreator));
    }

    public Flux<RabbitMQSenderResult<RabbitMQMessage<T>>> sendMessages(Publisher<RabbitMQMessage<T>> messages) {
        return futureResources
            .flatMapMany(resources -> sendMessages(resources, messages));
    }

    public Function<Publisher<Alo<T>>, AloFlux<RabbitMQSenderResult<T>>>
    sendAloBodies(RabbitMQMessageCreator<T> messageCreator) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator);
    }

    public AloFlux<RabbitMQSenderResult<T>>
    sendAloBodies(Publisher<Alo<T>> aloBodies, RabbitMQMessageCreator<T> messageCreator) {
        return futureResources
            .flatMapMany(resources -> sendAloBodies(resources, aloBodies, messageCreator))
            .as(AloFlux::wrap);
    }

    public AloFlux<RabbitMQSenderResult<RabbitMQMessage<T>>>
    sendAloMessages(Publisher<Alo<RabbitMQMessage<T>>> aloMessages) {
        return futureResources
            .flatMapMany(resources -> sendAloMessages(resources, aloMessages))
            .as(AloFlux::wrap);
    }

    private Flux<RabbitMQSenderResult<T>>
    sendBodies(SendResources<T> resources, Publisher<T> bodies, RabbitMQMessageCreator<T> messageCreator) {
        return Flux.from(bodies)
            .map(body -> resources.createOutboundMessage(messageCreator.apply(body), body))
            .transform(obMessages -> resources.getSender().sendWithTypedPublishConfirms(obMessages, SEND_OPTIONS))
            .map(RabbitMQSenderResult::fromMessageResult);
    }

    private Flux<RabbitMQSenderResult<RabbitMQMessage<T>>>
    sendMessages(SendResources<T> resources, Publisher<RabbitMQMessage<T>> messages) {
        return Flux.from(messages)
            .map(message -> resources.createOutboundMessage(message, message))
            .transform(obMessages -> resources.getSender().sendWithTypedPublishConfirms(obMessages, SEND_OPTIONS))
            .map(RabbitMQSenderResult::fromMessageResult);
    }

    private Flux<Alo<RabbitMQSenderResult<T>>>
    sendAloBodies(SendResources<T> resources, Publisher<Alo<T>> aloBodies, RabbitMQMessageCreator<T> messageCreator) {
        return AloFlux.toFlux(aloBodies)
            .map(aloBody -> resources.createOutboundMessage(messageCreator.apply(aloBody.get()), aloBody))
            .transform(obMessages -> resources.getSender().sendWithTypedPublishConfirms(obMessages, ALO_SEND_OPTIONS))
            .map(RabbitMQSenderResult::fromMessageResultOfAlo);
    }

    private Flux<Alo<RabbitMQSenderResult<RabbitMQMessage<T>>>>
    sendAloMessages(SendResources<T> resources, Publisher<Alo<RabbitMQMessage<T>>> aloMessages) {
        return AloFlux.toFlux(aloMessages)
            .map(aloMessage -> resources.createOutboundMessage(aloMessage.get(), aloMessage))
            .transform(obMessages -> resources.getSender().sendWithTypedPublishConfirms(obMessages, ALO_SEND_OPTIONS))
            .map(RabbitMQSenderResult::fromMessageResultOfAlo);
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
                config.loadConfiguredOrThrow(BODY_SERIALIZER_CONFIG));
        }

        public <C> CorrelableOutboundMessage<C>
        createOutboundMessage(RabbitMQMessage<T> message, C correlationMetadata) {
            SerializedBody serializedBody = bodySerializer.serialize(message.getBody());
            for (RabbitMQMessageSendInterceptor<T> interceptor : interceptors) {
                message = interceptor.onSend(message, serializedBody);
            }
            return new CorrelableOutboundMessage<>(
                message.getExchange(),
                message.getRoutingKey(),
                message.getProperties(),
                serializedBody.bytes(),
                correlationMetadata);
        }

        public Sender getSender() {
            return sender;
        }
    }
}
