package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFlux;
import io.atleon.util.Defaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * A reactive RabbitMQ receiver with at-least-once semantics for consuming messages from a queue in
 * a RabbitMQ cluster.
 * <p>
 * Note that {@link io.atleon.core.AloDecorator AloDecorators} applied via
 * {@link io.atleon.core.AloDecoratorConfig#ALO_DECORATOR_TYPES_CONFIG} must be
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

    private final Mono<Resources<T>> futureResources;

    private AloRabbitMQReceiver(RabbitMQConfigSource configSource) {
        this.futureResources = configSource.create()
            .map(Resources::<T>fromConfig)
            .cache();
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
        return futureResources
            .flatMapMany(resources -> receiveMessages(resources, queue))
            .as(AloFlux::wrap);
    }

    private Flux<Alo<ReceivedRabbitMQMessage<T>>> receiveMessages(Resources<T> resources, String queue) {
        Sinks.Empty<Alo<ReceivedRabbitMQMessage<T>>> sink = Sinks.empty();
        return resources.receive(queue, sink::tryEmitError)
            .mergeWith(sink.asMono());
    }

    private static final class Resources<T> {

        private final ConnectionFactory connectionFactory;

        private final int qos;

        private final BodyDeserializer<T> bodyDeserializer;

        private final NacknowledgerFactory<T> nacknowledgerFactory;

        private final AloFactory<ReceivedRabbitMQMessage<T>> aloFactory;

        private Resources(
            ConnectionFactory connectionFactory,
            int qos,
            BodyDeserializer<T> bodyDeserializer,
            NacknowledgerFactory<T> nacknowledgerFactory,
            AloFactory<ReceivedRabbitMQMessage<T>> aloFactory
        ) {
            this.connectionFactory = connectionFactory;
            this.qos = qos;
            this.bodyDeserializer = bodyDeserializer;
            this.nacknowledgerFactory = nacknowledgerFactory;
            this.aloFactory = aloFactory;
        }

        public static <T> Resources<T> fromConfig(RabbitMQConfig config) {
            return new Resources<T>(
                config.getConnectionFactory(),
                config.loadInt(QOS_CONFIG).orElse(Defaults.PREFETCH),
                config.loadConfiguredOrThrow(BODY_DESERIALIZER_CONFIG, BodyDeserializer.class),
                createNacknowledgerFactory(config),
                config.loadAloFactory()
            );
        }

        public Flux<Alo<ReceivedRabbitMQMessage<T>>> receive(String queue, Consumer<Throwable> errorEmitter) {
            ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory);

            ConsumeOptions consumeOptions = new ConsumeOptions()
                .qos(qos);

            return new Receiver(receiverOptions)
                .consumeManualAck(queue, consumeOptions)
                .map(delivery -> deserialize(queue, delivery, errorEmitter));
        }

        private Alo<ReceivedRabbitMQMessage<T>>
        deserialize(String queue, AcknowledgableDelivery delivery, Consumer<Throwable> errorEmitter) {
            SerializedBody body = SerializedBody.ofBytes(delivery.getBody());
            ReceivedRabbitMQMessage<T> message = new ReceivedRabbitMQMessage<>(
                queue,
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

        private void ack(AcknowledgableDelivery delivery, Consumer<? super Throwable> errorEmitter) {
            try {
                delivery.ack(false);
            } catch (Throwable error) {
                LOGGER.error("Failed to ack", error);
                errorEmitter.accept(error);
            }
        }

        private void nack(AcknowledgableDelivery delivery, boolean requeue, Consumer<? super Throwable> errorEmitter) {
            try {
                delivery.nack(false, requeue);
            } catch (Throwable fatalError) {
                LOGGER.error("Failed to nack", fatalError);
                errorEmitter.accept(fatalError);
            }
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
                    .<NacknowledgerFactory<T>>flatMap(Resources::instantiatePredefinedNacknowledgerFactory)
                    .orElseThrow(() -> new IllegalStateException("Failed to convert NackStrategy to NacknowledgerFactory"));
            }

            return new NacknowledgerFactory.Emit<>();
        }

        private static <T, N extends NacknowledgerFactory<T>> Optional<NacknowledgerFactory<T>>
        loadNacknowledgerFactory(RabbitMQConfig config, String key, Class<N> type) {
            return config.loadConfiguredWithPredefinedTypes(key, type, Resources::instantiatePredefinedNacknowledgerFactory);
        }

        private static <T> Optional<NacknowledgerFactory<T>> instantiatePredefinedNacknowledgerFactory(String typeName) {
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
    }
}
