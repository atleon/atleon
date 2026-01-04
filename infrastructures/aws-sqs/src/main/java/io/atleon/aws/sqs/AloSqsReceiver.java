package io.atleon.aws.sqs;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFactoryConfig;
import io.atleon.core.AloFlux;
import io.atleon.core.AloSignalListenerFactory;
import io.atleon.core.AloSignalListenerFactoryConfig;
import io.atleon.core.ErrorEmitter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * A reactive receiver of {@link Alo} items holding SQS Messages or Message bodies.
 * <p>
 * Each subscription to returned {@link AloFlux}s is backed by an
 * {@link software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient}. When a subscription
 * is terminated for any reason, the client is closed.
 * <p>
 * Note that {@link io.atleon.core.AloDecorator AloDecorators} applied via
 * {@link io.atleon.core.AloDecoratorConfig#DECORATOR_TYPES_CONFIG} must be
 * implementations of {@link AloReceivedSqsMessageDecorator}.
 *
 * @param <T> The deserialized type of SQS Message bodies
 */
public class AloSqsReceiver<T> {

    /**
     * Prefix used on all AloSqsReceiver-specific configurations.
     */
    public static final String CONFIG_PREFIX = "sqs.receiver.";

    /**
     * Qualified class name of a {@link BodyDeserializer} used to convert the String payload in SQS
     * Messages to other types. You can use {@link StringBodyDeserializer} if you just want the raw
     * String payload.
     */
    public static final String BODY_DESERIALIZER_CONFIG = CONFIG_PREFIX + "body.deserializer";

    /**
     * Configures the behavior of negatively acknowledging SQS Messages. Several simple types are
     * available including {@value #NACKNOWLEDGER_TYPE_EMIT}, where the associated error is emitted
     * in to the pipeline, and {@value #NACKNOWLEDGER_TYPE_VISIBILITY_RESET} which marks the
     * message as no longer in flight and resets its visibility such that it is either re-received
     * in the future or dead-lettered (note that when using {@value #NACKNOWLEDGER_TYPE_VISIBILITY_RESET},
     * the number of seconds the visibility is reset by is configurable through
     * {@link NacknowledgerFactory#VISIBILITY_RESET_SECONDS_CONFIG}, where the default is zero,
     * leading to the message being immediately re-receivable). Any other non-predefined value is
     * treated as a qualified class name of an implementation of {@link NacknowledgerFactory} which
     * allows more fine-grained control over what happens when an SQS Message is negatively
     * acknowledged. Defaults to "emit".
     */
    public static final String NACKNOWLEDGER_TYPE_CONFIG = CONFIG_PREFIX + "nacknowledger.type";

    public static final String NACKNOWLEDGER_TYPE_EMIT = "emit";

    public static final String NACKNOWLEDGER_TYPE_VISIBILITY_RESET = "visibility_reset";

    /**
     * When negative acknowledgement results in emitting the corresponding error, this configures
     * the timeout on successfully emitting that error.
     */
    public static final String ERROR_EMISSION_TIMEOUT_CONFIG = CONFIG_PREFIX + "error.emission.timeout";

    /**
     * Configures the maximum number of messages returned by each Receive Message Request to SQS.
     * Minimum is 1 and maximum is 10.
     */
    public static final String MAX_MESSAGES_PER_RECEPTION_CONFIG = CONFIG_PREFIX + "max.messages.per.reception";

    /**
     * List of Message Attributes to request on each message received from SQS.
     */
    public static final String MESSAGE_ATTRIBUTES_TO_REQUEST_CONFIG = CONFIG_PREFIX + "message.attributes.to.request";

    /**
     * List of Message System Attributes to request on each message received from SQS. For a full
     * list of available Attributes, see {@link software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName}.
     */
    public static final String MESSAGE_SYSTEM_ATTRIBUTES_TO_REQUEST_CONFIG =
            CONFIG_PREFIX + "message.system.attributes.to.request";

    /**
     * Configures the "wait time" (in seconds) for each Receive Message Request to SQS. Any value
     * greater than zero activates "long polling".
     */
    public static final String WAIT_TIME_SECONDS_PER_RECEPTION_CONFIG =
            CONFIG_PREFIX + "wait.time.seconds.per.reception";

    /**
     * Configures the visibility timeout (in seconds) for each received Message. Note that if
     * Messages are not acknowledged for long enough, their visibility timeout may lapse and may
     * be received again.
     */
    public static final String VISIBILITY_TIMEOUT_SECONDS_CONFIG = CONFIG_PREFIX + "visibility.timeout";

    /**
     * For each subscription to SQS Messages, this is the maximum number of non-acknowledged (and
     * non-nacknowledged) Messages. Note that if Messages are not acknowledged for long enough,
     * their visibility timeout may lapse and may be received again, and acknowledging the original
     * receipt may result in an error.
     */
    public static final String MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG = CONFIG_PREFIX + "max.in.flight.per.subscription";

    /**
     * The max number of Messages to delete in each SQS batch delete request. Batching is
     * effectively disabled when this value {@literal <=} 1.  When batching is enabled (batch size
     * {@literal >} 1 {@link #DELETE_BATCH_INTERVAL_CONFIG} must also be configured such that there
     * is an upper bound on how long a batch will remain open when waiting for it to be filled.
     */
    public static final String DELETE_BATCH_SIZE_CONFIG = CONFIG_PREFIX + "delete.batch.size";

    /**
     * When delete batching is enabled, this configures the maximum amount of time a batch will
     * remain open while waiting for it to be filled. Specified as an ISO-8601 Duration, e.g. PT1S
     */
    public static final String DELETE_BATCH_INTERVAL_CONFIG = CONFIG_PREFIX + "delete.batch.interval";

    /**
     * Upon termination of a subscription to SQS Messages, either due to errors or cancellation,
     * this is the amount of time to wait before closing the underlying SQS Client and propagating
     * the termination signal downstream. Specified as ISO-8601 Duration, e.g. PT10S
     */
    public static final String CLOSE_TIMEOUT_CONFIG = CONFIG_PREFIX + "close.timeout";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloSqsReceiver.class);

    private final SqsConfigSource configSource;

    private AloSqsReceiver(SqsConfigSource configSource) {
        this.configSource = configSource;
    }

    /**
     * Alias for {@link #create(SqsConfigSource)}. Will be deprecated in future release.
     */
    public static <T> AloSqsReceiver<T> from(SqsConfigSource configSource) {
        return create(configSource);
    }

    /**
     * Creates a new AloSqsReceiver from the provided {@link SqsConfigSource}
     *
     * @param configSource The reactive source of {@link SqsConfig}
     * @param <T>          The types of deserialized message bodies contained in received messages
     * @return A new AloSqsReceiver
     */
    public static <T> AloSqsReceiver<T> create(SqsConfigSource configSource) {
        return new AloSqsReceiver<>(configSource);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing deserialized SQS message bodies
     * wrapped as an {@link AloFlux}.
     *
     * @param queueUrl The URL of an SQS queue to subscribe to
     * @return A Publisher of Alo items referencing deserialized SQS message bodies
     */
    public AloFlux<T> receiveAloBodies(String queueUrl) {
        return receiveAloMessages(queueUrl).map(SqsMessage::body);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing {@link ReceivedSqsMessage}s wrapped as
     * an {@link AloFlux}.
     *
     * @param queueUrl The URL of an SQS queue to subscribe to
     * @return A Publisher of Alo items referencing {@link ReceivedSqsMessage}s
     */
    public AloFlux<ReceivedSqsMessage<T>> receiveAloMessages(String queueUrl) {
        return configSource
                .create()
                .map(ReceiveResources<T>::new)
                .flatMapMany(resources -> resources.receive(queueUrl))
                .as(AloFlux::wrap);
    }

    private static final class ReceiveResources<T> {

        private final SqsConfig config;

        private final BodyDeserializer<T> bodyDeserializer;

        private final NacknowledgerFactory<T> nacknowledgerFactory;

        public ReceiveResources(SqsConfig config) {
            this.config = config;
            this.bodyDeserializer = config.loadConfiguredOrThrow(BODY_DESERIALIZER_CONFIG, BodyDeserializer.class);
            this.nacknowledgerFactory = createNacknowledgerFactory(config);
        }

        public Flux<Alo<ReceivedSqsMessage<T>>> receive(String queueUrl) {
            AloFactory<ReceivedSqsMessage<T>> aloFactory = loadAloFactory(queueUrl);
            ErrorEmitter<Alo<ReceivedSqsMessage<T>>> errorEmitter = newErrorEmitter();
            return newReceiver()
                    .receiveManual(queueUrl)
                    .map(message -> deserialize(message, aloFactory, errorEmitter::safelyEmit))
                    .transform(errorEmitter::applyTo)
                    .transform(aloMessages -> applySignalListenerFactories(aloMessages, queueUrl));
        }

        private AloFactory<ReceivedSqsMessage<T>> loadAloFactory(String queueUrl) {
            Map<String, Object> factoryConfig = config.modifyAndGetProperties(
                    it -> it.put(AloReceivedSqsMessageDecorator.QUEUE_URL_CONFIG, queueUrl));
            return AloFactoryConfig.loadDecorated(factoryConfig, AloReceivedSqsMessageDecorator.class);
        }

        private ErrorEmitter<Alo<ReceivedSqsMessage<T>>> newErrorEmitter() {
            Duration timeout =
                    config.loadDuration(ERROR_EMISSION_TIMEOUT_CONFIG).orElse(ErrorEmitter.DEFAULT_TIMEOUT);
            return ErrorEmitter.create(timeout);
        }

        private SqsReceiver newReceiver() {
            SqsReceiverOptions receiverOptions = SqsReceiverOptions.newBuilder(config::buildClient)
                    .maxMessagesPerReception(config.loadInt(MAX_MESSAGES_PER_RECEPTION_CONFIG)
                            .orElse(SqsReceiverOptions.DEFAULT_MAX_MESSAGES_PER_RECEPTION))
                    .messageAttributesToRequest(config.loadSetOfStringOrEmpty(MESSAGE_ATTRIBUTES_TO_REQUEST_CONFIG))
                    .messageSystemAttributesToRequest(
                            config.loadSetOfStringOrEmpty(MESSAGE_SYSTEM_ATTRIBUTES_TO_REQUEST_CONFIG))
                    .waitTimeSecondsPerReception(config.loadInt(WAIT_TIME_SECONDS_PER_RECEPTION_CONFIG)
                            .orElse(SqsReceiverOptions.DEFAULT_WAIT_TIME_SECONDS_PER_RECEPTION))
                    .visibilityTimeoutSeconds(config.loadInt(VISIBILITY_TIMEOUT_SECONDS_CONFIG)
                            .orElse(SqsReceiverOptions.DEFAULT_VISIBILITY_TIMEOUT_SECONDS))
                    .maxInFlightPerSubscription(config.loadInt(MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG)
                            .orElse(SqsReceiverOptions.DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION))
                    .deleteBatchSize(config.loadInt(DELETE_BATCH_SIZE_CONFIG)
                            .orElse(SqsReceiverOptions.DEFAULT_DELETE_BATCH_SIZE))
                    .deleteInterval(config.loadDuration(DELETE_BATCH_INTERVAL_CONFIG)
                            .orElse(SqsReceiverOptions.DEFAULT_DELETE_INTERVAL))
                    .closeTimeout(
                            config.loadDuration(CLOSE_TIMEOUT_CONFIG).orElse(SqsReceiverOptions.DEFAULT_CLOSE_TIMEOUT))
                    .build();
            return SqsReceiver.create(receiverOptions);
        }

        private Flux<Alo<ReceivedSqsMessage<T>>> applySignalListenerFactories(
                Flux<Alo<ReceivedSqsMessage<T>>> aloMessages, String queueUrl) {
            Map<String, Object> factoryConfig = config.modifyAndGetProperties(properties ->
                    properties.put(AloReceivedSqsMessageSignalListenerFactory.QUEUE_URL_CONFIG, queueUrl));
            List<AloSignalListenerFactory<ReceivedSqsMessage<T>, ?>> factories =
                    AloSignalListenerFactoryConfig.loadList(
                            factoryConfig, AloReceivedSqsMessageSignalListenerFactory.class);
            for (AloSignalListenerFactory<ReceivedSqsMessage<T>, ?> factory : factories) {
                aloMessages = aloMessages.tap(factory);
            }
            return aloMessages;
        }

        private Alo<ReceivedSqsMessage<T>> deserialize(
                SqsReceiverMessage message,
                AloFactory<ReceivedSqsMessage<T>> aloFactory,
                Consumer<Throwable> errorEmitter) {
            ReceivedSqsMessage<T> deserialized = DeserializedSqsMessage.deserialize(message, bodyDeserializer);
            return aloFactory.create(
                    deserialized,
                    message.deleter(),
                    nacknowledgerFactory.create(deserialized, message.visibilityChanger(), errorEmitter));
        }

        private static <T> NacknowledgerFactory<T> createNacknowledgerFactory(SqsConfig config) {
            Optional<NacknowledgerFactory<T>> nacknowledgerFactory =
                    loadNacknowledgerFactory(config, NACKNOWLEDGER_TYPE_CONFIG, NacknowledgerFactory.class);
            return nacknowledgerFactory.orElseGet(NacknowledgerFactory.Emit::new);
        }

        private static <T, N extends NacknowledgerFactory<T>>
                Optional<NacknowledgerFactory<T>> loadNacknowledgerFactory(
                        SqsConfig config, String key, Class<N> type) {
            return config.loadConfiguredWithPredefinedTypes(
                    key, type, ReceiveResources::newPredefinedNacknowledgerFactory);
        }

        private static <T> Optional<NacknowledgerFactory<T>> newPredefinedNacknowledgerFactory(String typeName) {
            if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_EMIT)) {
                return Optional.of(new NacknowledgerFactory.Emit<>());
            } else if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_VISIBILITY_RESET)) {
                return Optional.of(new NacknowledgerFactory.VisibilityReset<>(LOGGER));
            } else {
                return Optional.empty();
            }
        }
    }
}
