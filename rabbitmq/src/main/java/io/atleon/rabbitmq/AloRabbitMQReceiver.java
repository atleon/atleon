package io.atleon.rabbitmq;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.core.AloFlux;
import io.atleon.core.AloSignalListenerFactory;
import io.atleon.core.AloSignalListenerFactoryConfig;
import io.atleon.util.Defaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * A reactive RabbitMQ receiver with at-least-once semantics for consuming messages from a queue in
 * a RabbitMQ cluster.
 * <p>
 * Note that {@link io.atleon.core.AloDecorator AloDecorators} applied via
 * {@link io.atleon.core.AloDecoratorConfig#DECORATOR_TYPES_CONFIG} must be
 * implementations of {@link AloReceivedRabbitMQMessageDecorator}.
 *
 * @param <T> Inbound message deserialized body type
 */
public class AloRabbitMQReceiver<T> {

    /**
     * @deprecated Use {@link #NACKNOWLEDGER_TYPE_EMIT}, {@link #NACKNOWLEDGER_TYPE_REQUEUE}, or
     * {@link #NACKNOWLEDGER_TYPE_DISCARD}
     */
    @Deprecated
    public enum NackStrategy {EMIT, REQUEUE, DISCARD}

    /**
     * Prefix used on all AloRabbitMQReceiver-specific configurations
     */
    public static final String CONFIG_PREFIX = "rabbitmq.receiver.";

    /**
     * The maximum allowed number unacknowledged messages (per subscription)
     */
    public static final String QOS_CONFIG = CONFIG_PREFIX + "qos";

    /**
     * An implementation of {@link BodyDeserializer} used to deserialized message bodies
     */
    public static final String BODY_DESERIALIZER_CONFIG = CONFIG_PREFIX + "body.deserializer";

    /**
     * @deprecated Use {@link #NACKNOWLEDGER_TYPE_CONFIG}
     */
    @Deprecated
    public static final String NACK_STRATEGY_CONFIG = CONFIG_PREFIX + "nack.strategy";

    /**
     * Configures the behavior of negatively acknowledging SQS Messages. Several simple types are
     * available including {@value #NACKNOWLEDGER_TYPE_EMIT}, where the associated error is emitted
     * in to the pipeline, {@value #NACKNOWLEDGER_TYPE_REQUEUE} which re-queues the associated
     * message such that it is re-received in the future, and {@value #NACKNOWLEDGER_TYPE_DISCARD}
     * which causes the message to be dropped or routed to a dead-letter queue, if configured. Any
     * other non-predefined value is treated as a qualified class name of an implementation of
     * {@link NacknowledgerFactory} which allows more fine-grained control over what happens when
     * a RabbitMQ Message is negatively acknowledged. Defaults to "emit".
     */
    public static final String NACKNOWLEDGER_TYPE_CONFIG = CONFIG_PREFIX + "nacknowledger.type";

    public static final String NACKNOWLEDGER_TYPE_EMIT = "emit";

    public static final String NACKNOWLEDGER_TYPE_REQUEUE = "requeue";

    public static final String NACKNOWLEDGER_TYPE_DISCARD = "discard";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloRabbitMQReceiver.class);

    private final RabbitMQConfigSource configSource;

    private AloRabbitMQReceiver(RabbitMQConfigSource configSource) {
        this.configSource = configSource;
    }

    /**
     * Creates a new AloRabbitMQReceiver from the provided {@link RabbitMQConfigSource}
     *
     * @param configSource The reactive source of {@link RabbitMQConfig}
     * @param <T>          The types of deserialized message bodies contained in received messages
     * @return A new AloRabbitMQReceiver
     */
    public static <T> AloRabbitMQReceiver<T> from(RabbitMQConfigSource configSource) {
        return new AloRabbitMQReceiver<>(configSource);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing deserialized RabbitMQ message bodies
     * wrapped as an {@link AloFlux}.
     *
     * @param queue The queue to subscribe to
     * @return A Publisher of Alo items referencing deserialized RabbitMQ message bodies
     */
    public AloFlux<T> receiveAloBodies(String queue) {
        return receiveAloMessages(queue)
            .mapNotNull(ReceivedRabbitMQMessage::getBody);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing {@link ReceivedRabbitMQMessage}s
     * wrapped as an {@link AloFlux}.
     *
     * @param queue The queue to subscribe to
     * @return A Publisher of Alo items referencing ReceivedRabbitMQMessages
     */
    public AloFlux<ReceivedRabbitMQMessage<T>> receiveAloMessages(String queue) {
        return configSource.create()
            .map(ReceiveResources<T>::new)
            .flatMapMany(resources -> resources.receive(queue))
            .as(AloFlux::wrap);
    }

    private static final class ReceiveResources<T> {

        private final RabbitMQConfig config;

        private final BodyDeserializer<T> bodyDeserializer;

        private final NacknowledgerFactory<T> nacknowledgerFactory;

        public ReceiveResources(RabbitMQConfig config) {
            this.config = config;
            this.bodyDeserializer = config.loadConfiguredOrThrow(BODY_DESERIALIZER_CONFIG, BodyDeserializer.class);
            this.nacknowledgerFactory = createNacknowledgerFactory(config);
        }

        public Flux<Alo<ReceivedRabbitMQMessage<T>>> receive(String queue) {
            AloFactory<ReceivedRabbitMQMessage<T>> aloFactory = loadAloFactory(queue);
            Sinks.Empty<Alo<ReceivedRabbitMQMessage<T>>> sink = Sinks.empty();
            return newReceiver().consumeManualAck(queue, newConsumeOptions())
                .map(delivery -> deserialize(delivery, aloFactory, sink::tryEmitError))
                .mergeWith(sink.asMono())
                .transform(aloMessages -> applySignalListenerFactories(aloMessages, queue));
        }

        private AloFactory<ReceivedRabbitMQMessage<T>> loadAloFactory(String queue) {
            return AloFactoryConfig.loadDecorated(
                config.modifyAndGetProperties(it -> it.put(AloReceivedRabbitMQMessageDecorator.QUEUE_CONFIG, queue)),
                AloReceivedRabbitMQMessageDecorator.class
            );
        }

        private Receiver newReceiver() {
            ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(config.getConnectionFactory());
            return new Receiver(receiverOptions);
        }

        private ConsumeOptions newConsumeOptions() {
            return new ConsumeOptions()
                .qos(config.loadInt(QOS_CONFIG).orElse(Defaults.PREFETCH));
        }

        private Flux<Alo<ReceivedRabbitMQMessage<T>>>
        applySignalListenerFactories(Flux<Alo<ReceivedRabbitMQMessage<T>>> aloMessages, String queue) {
            Map<String, Object> factoryConfig = config.modifyAndGetProperties(properties ->
                properties.put(AloReceivedRabbitMQMessageSignalListenerFactory.QUEUE_CONFIG, queue)
            );
            List<AloSignalListenerFactory<ReceivedRabbitMQMessage<T>, ?>> factories =
                AloSignalListenerFactoryConfig.loadList(factoryConfig, AloReceivedRabbitMQMessageSignalListenerFactory.class);
            for (AloSignalListenerFactory<ReceivedRabbitMQMessage<T>, ?> factory : factories) {
                aloMessages = aloMessages.tap(factory);
            }
            return aloMessages;
        }

        private Alo<ReceivedRabbitMQMessage<T>> deserialize(
            AcknowledgableDelivery delivery,
            AloFactory<ReceivedRabbitMQMessage<T>> aloFactory,
            Consumer<Throwable> errorEmitter
        ) {
            SerializedBody body = SerializedBody.ofBytes(delivery.getBody());
            ReceivedRabbitMQMessage<T> message = new ReceivedRabbitMQMessage<>(
                delivery.getEnvelope().getExchange(),
                delivery.getEnvelope().getRoutingKey(),
                delivery.getProperties(),
                bodyDeserializer.deserialize(body)
            );

            return aloFactory.create(
                message,
                () -> ack(delivery, errorEmitter),
                nacknowledgerFactory.create(message, requeue -> nack(delivery, requeue, errorEmitter), errorEmitter)
            );
        }

        private static <T> NacknowledgerFactory<T> createNacknowledgerFactory(RabbitMQConfig config) {
            Optional<NacknowledgerFactory<T>> nacknowledgerFactory =
                loadNacknowledgerFactory(config, NACKNOWLEDGER_TYPE_CONFIG, NacknowledgerFactory.class);
            if (nacknowledgerFactory.isPresent()) {
                return nacknowledgerFactory.get();
            }

            Optional<NackStrategy> deprecatedNackStrategy =
                config.loadParseable(NACK_STRATEGY_CONFIG, NackStrategy.class, NackStrategy::valueOf);
            if (deprecatedNackStrategy.isPresent()) {
                LOGGER.warn("The configuration " + NACK_STRATEGY_CONFIG + " is deprecated. Use " + NACKNOWLEDGER_TYPE_CONFIG);
                return deprecatedNackStrategy.map(Enum::name)
                    .<NacknowledgerFactory<T>>flatMap(ReceiveResources::newPredefinedNacknowledgerFactory)
                    .orElseThrow(() -> new IllegalStateException("Failed to convert NackStrategy to NacknowledgerFactory"));
            }

            return new NacknowledgerFactory.Emit<>();
        }

        private static <T, N extends NacknowledgerFactory<T>> Optional<NacknowledgerFactory<T>>
        loadNacknowledgerFactory(RabbitMQConfig config, String key, Class<N> type) {
            return config.loadConfiguredWithPredefinedTypes(key, type, ReceiveResources::newPredefinedNacknowledgerFactory);
        }

        private static <T> Optional<NacknowledgerFactory<T>> newPredefinedNacknowledgerFactory(String typeName) {
            if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_EMIT)) {
                return Optional.of(new NacknowledgerFactory.Emit<>());
            } else if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_REQUEUE)) {
                return Optional.of(new NacknowledgerFactory.Nack<>(LOGGER, true));
            } else if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_DISCARD)) {
                return Optional.of(new NacknowledgerFactory.Nack<>(LOGGER, false));
            } else {
                return Optional.empty();
            }
        }

        private static void ack(AcknowledgableDelivery delivery, Consumer<? super Throwable> errorEmitter) {
            try {
                delivery.ack(false);
            } catch (Throwable error) {
                LOGGER.error("Failed to ack", error);
                errorEmitter.accept(error);
            }
        }

        private static void nack(AcknowledgableDelivery delivery, boolean requeue, Consumer<? super Throwable> errorEmitter) {
            try {
                delivery.nack(false, requeue);
            } catch (Throwable fatalError) {
                LOGGER.error("Failed to nack", fatalError);
                errorEmitter.accept(fatalError);
            }
        }
    }
}
