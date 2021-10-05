package io.atleon.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;
import io.atleon.core.Alo;
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

import java.util.function.Consumer;

/**
 * A reactive RabbitMQ receiver with at-least-once semantics for consuming messages from a queue in
 * a RabbitMQ cluster
 *
 * @param <T> inbound message deserialized body type
 */
public class AloRabbitMQReceiver<T> {

    /**
     * Strategy for handling Nacknowledgement
     * - EMIT causes error to be emitted to subscribers
     * - REQUEUE causes nacknowledged message to be nack'd with requeue
     * - DISCARD causes nacknowledged message to be nack'ed with discard
     * Default is EMIT
     */
    public enum NackStrategy {EMIT, REQUEUE, DISCARD}

    /**
     * Prefix used on all AloRabbitMQReceiver-specific configurations
     */
    public static final String CONFIG_PREFIX = "rabbitmq-receiver-";

    /**
     * The maximum allowed number unacknowledged messages (per subscription)
     */
    public static final String QOS_CONFIG = CONFIG_PREFIX + "qos";

    /**
     * An implementation of {@link BodyDeserializer} used to deserialized message bodies
     */
    public static final String BODY_DESERIALIZER_CONFIG = CONFIG_PREFIX + "body-deserializer";

    /**
     * Strategy used for handling Nacknowledgement. See {@link NackStrategy}
     */
    public static final String NACK_STRATEGY_CONFIG = CONFIG_PREFIX + "nack-strategy";

    /**
     * An implementation of {@link AloRabbitMQMessageFactory} used to wrap messages in an
     * implementation of {@link Alo}. Default is {@link DefaultAloRabbitMQMessageFactory}
     */
    public static final String ALO_FACTORY_CONFIG = CONFIG_PREFIX + "alo-factory";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloRabbitMQReceiver.class);

    private final Mono<ReceiveResources<T>> futureResources;

    private AloRabbitMQReceiver(RabbitMQConfigSource configSource) {
        this.futureResources = configSource.create()
            .map(ReceiveResources::<T>fromConfig)
            .cache();
    }

    public static <T> AloRabbitMQReceiver<T> from(RabbitMQConfigSource configSource) {
        return new AloRabbitMQReceiver<>(configSource);
    }

    public AloFlux<T> receiveAloBodies(String queue) {
        return receiveAloMessages(queue)
            .filter(message -> message.getBody() != null)
            .map(RabbitMQMessage::getBody);
    }

    public AloFlux<RabbitMQMessage<T>> receiveAloMessages(String queue) {
        return futureResources
            .flatMapMany(resources -> receiveMessages(resources, queue))
            .as(AloFlux::wrap);
    }

    private Flux<Alo<RabbitMQMessage<T>>> receiveMessages(ReceiveResources<T> resources, String queue) {
        Sinks.Empty<Alo<RabbitMQMessage<T>>> sink = Sinks.empty();
        return resources.receive(queue, sink::tryEmitError)
            .mergeWith(sink.asMono());
    }

    private static final class ReceiveResources<T> {

        private final ConnectionFactory connectionFactory;

        private final int qos;

        private final BodyDeserializer<T> bodyDeserializer;

        private final NackStrategy nackStrategy;

        private final AloRabbitMQMessageFactory<T> messageFactory;

        private ReceiveResources(
            ConnectionFactory connectionFactory,
            int qos,
            BodyDeserializer<T> bodyDeserializer,
            NackStrategy nackStrategy,
            AloRabbitMQMessageFactory<T> messageFactory) {
            this.connectionFactory = connectionFactory;
            this.qos = qos;
            this.bodyDeserializer = bodyDeserializer;
            this.nackStrategy = nackStrategy;
            this.messageFactory = messageFactory;
        }

        public static <T> ReceiveResources<T> fromConfig(RabbitMQConfig config) {
            AloRabbitMQMessageFactory<T> messageFactory =
                config.<AloRabbitMQMessageFactory<T>>loadConfigured(ALO_FACTORY_CONFIG)
                    .orElseGet(DefaultAloRabbitMQMessageFactory::new);
            return new ReceiveResources<T>(
                config.getConnectionFactory(),
                config.load(QOS_CONFIG, Integer::parseInt).orElse(Defaults.PREFETCH),
                config.loadConfiguredOrThrow(BODY_DESERIALIZER_CONFIG),
                config.load(NACK_STRATEGY_CONFIG, NackStrategy::valueOf).orElse(NackStrategy.EMIT),
                messageFactory);
        }

        public Flux<Alo<RabbitMQMessage<T>>> receive(String queue, Consumer<? super Throwable> errorEmitter) {
            ReceiverOptions receiverOptions = new ReceiverOptions()
                .connectionFactory(connectionFactory);

            ConsumeOptions consumeOptions = new ConsumeOptions()
                .qos(qos);

            return new Receiver(receiverOptions)
                .consumeManualAck(queue, consumeOptions)
                .map(delivery -> deserialize(delivery, errorEmitter));
        }

        private Alo<RabbitMQMessage<T>>
        deserialize(AcknowledgableDelivery delivery, Consumer<? super Throwable> errorEmitter) {
            SerializedBody body = SerializedBody.ofBytes(delivery.getBody());
            RabbitMQMessage<T> rabbitMessage = new RabbitMQMessage<>(
                delivery.getEnvelope().getExchange(),
                delivery.getEnvelope().getRoutingKey(),
                delivery.getProperties(),
                bodyDeserializer.deserialize(body));

            Runnable acknowledger = () -> ack(delivery, errorEmitter);
            Consumer<? super Throwable> nacknowledger = error -> nack(delivery, errorEmitter, error);
            return messageFactory.create(rabbitMessage, acknowledger, nacknowledger);
        }

        private void ack(AcknowledgableDelivery delivery, Consumer<? super Throwable> errorEmitter) {
            try {
                delivery.ack(false);
            } catch (Throwable error) {
                LOGGER.error("Failed to ack", error);
                errorEmitter.accept(error);
            }
        }

        private void nack(AcknowledgableDelivery delivery, Consumer<? super Throwable> errorEmitter, Throwable error) {
            if (nackStrategy == NackStrategy.EMIT) {
                errorEmitter.accept(error);
            } else {
                try {
                    delivery.nack(false, nackStrategy == NackStrategy.REQUEUE);
                } catch (Throwable fatalError) {
                    LOGGER.error("Failed to nack", fatalError);
                    fatalError.addSuppressed(error);
                    errorEmitter.accept(fatalError);
                }
            }
        }
    }
}
