package io.atleon.aws.sqs;

import io.atleon.core.Alo;
import io.atleon.core.AloFactory;
import io.atleon.core.AloFlux;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * A reactive receiver of {@link Alo} items holding SQS Messages or Message bodies.
 * <p>
 * Each subscription to returned {@link AloFlux}s is backed by an
 * {@link software.amazon.awssdk.services.sqs.SqsAsyncClient SqsAsyncClient}. When a subscription
 * is terminated for any reason, the client is closed.
 * <p>
 * Note that {@link io.atleon.core.AloDecorator AloDecorators} applied via
 * {@link io.atleon.core.AloDecoratorConfig#ALO_DECORATOR_TYPES_CONFIG} must be
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
    public static final String MESSAGE_SYSTEM_ATTRIBUTES_TO_REQUEST_CONFIG = CONFIG_PREFIX + "message.system.attributes.to.request";

    /**
     * Configures the "wait time" (in seconds) for each Receive Message Request to SQS. Any value
     * greater than zero activates "long polling".
     */
    public static final String WAIT_TIME_SECONDS_PER_RECEPTION_CONFIG = CONFIG_PREFIX + "wait.time.seconds.per.reception";

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
     * Creates a new AloSqsReceiver from the provided {@link SqsConfigSource}
     *
     * @param configSource The reactive source of {@link SqsConfig}
     * @param <T>          The types of deserialized message bodies contained in received messages
     * @return A new AloSqsReceiver
     */
    public static <T> AloSqsReceiver<T> from(SqsConfigSource configSource) {
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
        return receiveAloMessages(queueUrl)
            .map(SqsMessage::body);
    }

    /**
     * Creates a Publisher of {@link Alo} items referencing {@link ReceivedSqsMessage}s wrapped as
     * an {@link AloFlux}.
     *
     * @param queueUrl The URL of an SQS queue to subscribe to
     * @return A Publisher of Alo items referencing {@link ReceivedSqsMessage}s
     */
    public AloFlux<ReceivedSqsMessage<T>> receiveAloMessages(String queueUrl) {
        return configSource.create()
            .flatMapMany(config -> receiveMessages(config, queueUrl))
            .as(AloFlux::wrap);
    }

    private Flux<Alo<ReceivedSqsMessage<T>>> receiveMessages(SqsConfig config, String queueUrl) {
        SqsReceiverOptions options = newReceiverOptions(config);
        AloFactory<ReceivedSqsMessage<T>> aloFactory = config.loadAloFactory();
        BodyDeserializer<T> bodyDeserializer = config.loadConfiguredOrThrow(BODY_DESERIALIZER_CONFIG, BodyDeserializer.class);
        NacknowledgerFactory<T> nacknowledgerFactory = createNacknowledgerFactory(config);

        Sinks.Empty<SqsReceiverMessage> sink = Sinks.empty();
        return SqsReceiver.create(options).receiveManual(queueUrl)
            .mergeWith(sink.asMono())
            .map(message -> toAlo(aloFactory, message, bodyDeserializer, nacknowledgerFactory, sink::tryEmitError));
    }

    private static SqsReceiverOptions newReceiverOptions(SqsConfig config) {
        return SqsReceiverOptions.newBuilder(config::buildClient)
            .maxMessagesPerReception(config.loadInt(MAX_MESSAGES_PER_RECEPTION_CONFIG).orElse(SqsReceiverOptions.DEFAULT_MAX_MESSAGES_PER_RECEPTION))
            .messageAttributesToRequest(config.loadSetOfStringOrEmpty(MESSAGE_ATTRIBUTES_TO_REQUEST_CONFIG))
            .messageSystemAttributesToRequest(config.loadSetOfStringOrEmpty(MESSAGE_SYSTEM_ATTRIBUTES_TO_REQUEST_CONFIG))
            .waitTimeSecondsPerReception(config.loadInt(WAIT_TIME_SECONDS_PER_RECEPTION_CONFIG).orElse(SqsReceiverOptions.DEFAULT_WAIT_TIME_SECONDS_PER_RECEPTION))
            .visibilityTimeoutSeconds(config.loadInt(VISIBILITY_TIMEOUT_SECONDS_CONFIG).orElse(SqsReceiverOptions.DEFAULT_VISIBILITY_TIMEOUT_SECONDS))
            .maxInFlightPerSubscription(config.loadInt(MAX_IN_FLIGHT_PER_SUBSCRIPTION_CONFIG).orElse(SqsReceiverOptions.DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION))
            .deleteBatchSize(config.loadInt(DELETE_BATCH_SIZE_CONFIG).orElse(SqsReceiverOptions.DEFAULT_DELETE_BATCH_SIZE))
            .deleteInterval(config.loadDuration(DELETE_BATCH_INTERVAL_CONFIG).orElse(SqsReceiverOptions.DEFAULT_DELETE_INTERVAL))
            .closeTimeout(config.loadDuration(CLOSE_TIMEOUT_CONFIG).orElse(SqsReceiverOptions.DEFAULT_CLOSE_TIMEOUT))
            .build();
    }

    private static <T> NacknowledgerFactory<T> createNacknowledgerFactory(SqsConfig config) {
        Optional<NacknowledgerFactory<T>> nacknowledgerFactory = config.loadNacknowledgerFactory(
            NACKNOWLEDGER_TYPE_CONFIG,
            NacknowledgerFactory.class,
            AloSqsReceiver::instantiatePredefinedNacknowledgerFactory
        );
        return nacknowledgerFactory.orElseGet(NacknowledgerFactory.Emit::new);
    }

    private static <T> Optional<NacknowledgerFactory<T>> instantiatePredefinedNacknowledgerFactory(String typeName) {
        if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_EMIT)) {
            return Optional.of(new NacknowledgerFactory.Emit<>());
        } else if (typeName.equalsIgnoreCase(NACKNOWLEDGER_TYPE_VISIBILITY_RESET)) {
            return Optional.of(new NacknowledgerFactory.VisibilityReset<>(LOGGER));
        } else {
            return Optional.empty();
        }
    }

    private static <T> Alo<ReceivedSqsMessage<T>> toAlo(
        AloFactory<ReceivedSqsMessage<T>> aloFactory,
        SqsReceiverMessage message,
        BodyDeserializer<T> bodyDeserializer,
        NacknowledgerFactory<T> nacknowledgerFactory,
        Consumer<Throwable> errorEmitter
    ) {
        ReceivedSqsMessage<T> deserialized = DeserializedSqsMessage.create(
            message.receiptHandle(),
            message.messageId(),
            message.messageAttributes(),
            message.messageSystemAttributes(),
            bodyDeserializer.deserialize(message.body())
        );
        return aloFactory.create(
            deserialized,
            message.deleter(),
            nacknowledgerFactory.create(deserialized, message.deleter(), message.visibilityChanger(), errorEmitter)
        );
    }
}
