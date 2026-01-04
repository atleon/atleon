package io.atleon.aws.sqs;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.core.SenderResult;
import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;

/**
 * A reactive sender of {@link Alo} data to SQS queues, with forwarding methods for non-Alo items.
 * <p>
 * At most one instance of a {@link SqsSender} is kept and can be closed upon invoking
 * {@link AloSqsSender#close()}. However, if after closing, more sent Publishers are subscribed to,
 * a new Sender instance will be created and cached.
 *
 * @param <T> The un-serialized type of SQS Message bodies
 */
public class AloSqsSender<T> implements Closeable {

    /**
     * Prefix used on all AloSqsSender-specific configurations.
     */
    public static final String CONFIG_PREFIX = "sqs.sender.";

    /**
     * Qualified class name of {@link BodySerializer} implementation used to serialize Message
     * bodies.
     */
    public static final String BODY_SERIALIZER_CONFIG = CONFIG_PREFIX + "body.serializer";

    /**
     * The max number of Messages to send in each SQS batch send request. Defaults to 1, meaning
     * batching is disabled and each Message is sent in its own request. When batching is enabled
     * (batch size {@literal >} 1), {@link #BATCH_DURATION_CONFIG} must also be configured such
     * that there is an upper bound on how long a batch will remain open when waiting for it to be
     * filled.
     */
    public static final String BATCH_SIZE_CONFIG = CONFIG_PREFIX + "batch.size";

    /**
     * When batching is enabled, this configures the maximum amount of time a batch will remain
     * open while waiting for it to be filled. Specified as an ISO-8601 Duration, e.g. PT0.1S
     */
    public static final String BATCH_DURATION_CONFIG = CONFIG_PREFIX + "batch.duration";

    /**
     * The number of batches to prefetch for sending. Note that the resulting upstream prefetch
     * will be the product of batch size and batch prefetch.
     */
    public static final String BATCH_PREFETCH_CONFIG = CONFIG_PREFIX + "batch.prefetch";

    /**
     * Configures the maximum number of SQS Requests in flight per sent Publisher. When batching is
     * disabled, this equates to the maximum number of Messages concurrently being sent. When
     * batching is enabled, this equates to the maximum number batches concurrently being sent.
     */
    public static final String MAX_REQUESTS_IN_FLIGHT_CONFIG = CONFIG_PREFIX + "max.requests.in.flight";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloSqsSender.class);

    private final Mono<SendResources<T>> futureResources;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private AloSqsSender(SqsConfigSource configSource) {
        this.futureResources = configSource
                .create()
                .map(SendResources::<T>fromConfig)
                .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), SendResources::close);
    }

    /**
     * Alias for {@link #create(SqsConfigSource)}. Will be deprecated in future release.
     */
    public static <T> AloSqsSender<T> from(SqsConfigSource configSource) {
        return create(configSource);
    }

    /**
     * Creates a new AloSqsSender from the provided {@link SqsConfigSource}
     *
     * @param configSource The reactive source of {@link SqsConfig}
     * @param <T>          The type of messages bodies sent by this sender
     * @return A new AloSqsSender
     */
    public static <T> AloSqsSender<T> create(SqsConfigSource configSource) {
        return new AloSqsSender<>(configSource);
    }

    /**
     * Sends a sequence of message bodies to be populated in {@link SqsMessage}s to the specified
     * queue URL.
     * <p>
     * The output of each sent message body is an {@link SqsSenderResult} containing the sent
     * value.
     *
     * @param bodies         A Publisher of SQS message bodies
     * @param messageCreator A factory that creates {@link SqsMessage}s from message bodies
     * @param queueUrl       URL of the queue to send messages to
     * @return a Publisher of the results of each sent message
     */
    public Flux<SqsSenderResult<T>> sendBodies(
            Publisher<T> bodies, SqsMessageCreator<T> messageCreator, String queueUrl) {
        return futureResources.flatMapMany(resources -> resources.send(bodies, messageCreator, queueUrl));
    }

    /**
     * Send a single {@link SqsMessage}
     *
     * @param message  A message to send
     * @param queueUrl URL of the queue to send the message to
     * @return A Publisher of the result of sending the message
     */
    public Mono<SqsSenderResult<SqsMessage<T>>> sendMessage(SqsMessage<T> message, String queueUrl) {
        return futureResources.flatMap(resources -> resources.send(message, queueUrl));
    }

    /**
     * Sends a sequence of {@link SqsMessage}s
     * <p>
     * The output of each sent message is an {@link SqsSenderResult} containing the sent
     * message.
     *
     * @param messages A Publisher of messages to send
     * @param queueUrl URL of the queue to send messages to
     * @return A Publisher of items referencing the result of each sent message
     */
    public Flux<SqsSenderResult<SqsMessage<T>>> sendMessages(Publisher<SqsMessage<T>> messages, String queueUrl) {
        return futureResources.flatMapMany(resources -> resources.send(messages, Function.identity(), queueUrl));
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing SQS message bodies to a Publisher of Alo items referencing the result of
     * sending each message body. See {@link #sendAloBodies(Publisher, SqsMessageCreator, String)}
     * for further information.
     *
     * @param messageCreator A factory that creates {@link SqsMessage}s from message bodies
     * @param queueUrl URL of the queue to send messages to
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<T>>, AloFlux<SqsSenderResult<T>>> sendAloBodies(
            SqsMessageCreator<T> messageCreator, String queueUrl) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator, queueUrl);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing message bodies to be populated in
     * {@link SqsMessage}s to the specified queue URL.
     * <p>
     * The output of each sent message body is an {@link SqsSenderResult} containing the sent
     * value. Each emitted item is an {@link Alo} item referencing an {@link SqsSenderResult}
     * and must be acknowledged or nacknowledged such that its processing can be marked complete at
     * the origin of the message.
     *
     * @param aloBodies      A Publisher of Alo items referencing SQS message bodies
     * @param messageCreator A factory that creates {@link SqsMessage}s from message bodies
     * @param queueUrl       URL of the queue to send messages to
     * @return a Publisher of Alo items referencing the result of each sent message
     */
    public AloFlux<SqsSenderResult<T>> sendAloBodies(
            Publisher<Alo<T>> aloBodies, SqsMessageCreator<T> messageCreator, String queueUrl) {
        return futureResources
                .flatMapMany(resources -> resources.sendAlos(aloBodies, messageCreator, queueUrl))
                .as(AloFlux::wrap)
                .processFailure(SenderResult::isFailure, SenderResult::toError);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing SQS messages to a Publisher of Alo items referencing the result of sending each
     * message. See {@link #sendAloMessages(Publisher, String)} for further information.
     *
     * @param queueUrl URL of the queue to send messages to
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<SqsMessage<T>>>, AloFlux<SqsSenderResult<SqsMessage<T>>>> sendAloMessages(
            String queueUrl) {
        return aloMessages -> sendAloMessages(aloMessages, queueUrl);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing {@link SqsMessage}s
     * <p>
     * The output of each sent message is an {@link SqsSenderResult} containing the sent
     * message. Each emitted item is an {@link Alo} item referencing a {@link SqsSenderResult}
     * and must be acknowledged or nacknowledged such that its processing can be marked complete at
     * the origin of the message.
     *
     * @param aloMessages A Publisher of Alo items referencing messages to send
     * @param queueUrl    URL of the queue to send messages to
     * @return A Publisher of Alo items referencing the result of each sent message
     */
    public AloFlux<SqsSenderResult<SqsMessage<T>>> sendAloMessages(
            Publisher<Alo<SqsMessage<T>>> aloMessages, String queueUrl) {
        return futureResources
                .flatMapMany(resources -> resources.sendAlos(aloMessages, Function.identity(), queueUrl))
                .as(AloFlux::wrap)
                .processFailure(SenderResult::isFailure, SenderResult::toError);
    }

    /**
     * Closes this sender and logs the provided reason.
     *
     * @param reason The reason this sender is being closed
     */
    public void close(Object reason) {
        LOGGER.info("Closing AloSqsSender due to reason={}", reason);
        close();
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private static final class SendResources<T> {

        private final SqsSender sender;

        private final BodySerializer<T> bodySerializer;

        private SendResources(SqsSender sender, BodySerializer<T> bodySerializer) {
            this.sender = sender;
            this.bodySerializer = bodySerializer;
        }

        public static <T> SendResources<T> fromConfig(SqsConfig config) {
            SqsSenderOptions options = SqsSenderOptions.newBuilder(config::buildClient)
                    .batchSize(config.loadInt(BATCH_SIZE_CONFIG).orElse(SqsSenderOptions.DEFAULT_BATCH_SIZE))
                    .batchDuration(
                            config.loadDuration(BATCH_DURATION_CONFIG).orElse(SqsSenderOptions.DEFAULT_BATCH_DURATION))
                    .batchPrefetch(
                            config.loadInt(BATCH_PREFETCH_CONFIG).orElse(SqsSenderOptions.DEFAULT_BATCH_PREFETCH))
                    .maxRequestsInFlight(config.loadInt(MAX_REQUESTS_IN_FLIGHT_CONFIG)
                            .orElse(SqsSenderOptions.DEFAULT_MAX_REQUESTS_IN_FLIGHT))
                    .build();
            return new SendResources<T>(
                    SqsSender.create(options),
                    config.loadConfiguredOrThrow(BODY_SERIALIZER_CONFIG, BodySerializer.class));
        }

        public Mono<SqsSenderResult<SqsMessage<T>>> send(SqsMessage<T> message, String queueUrl) {
            return sender.send(toSenderMessage(message, Function.identity()), queueUrl);
        }

        public <R> Flux<SqsSenderResult<R>> send(
                Publisher<R> items, Function<R, SqsMessage<T>> messageCreator, String queueUrl) {
            return Flux.from(items)
                    .map(item -> toSenderMessage(item, messageCreator))
                    .transform(senderMessages -> sender.send(senderMessages, queueUrl));
        }

        public <R> Flux<Alo<SqsSenderResult<R>>> sendAlos(
                Publisher<Alo<R>> alos, Function<R, SqsMessage<T>> messageCreator, String queueUrl) {
            return AloFlux.toFlux(alos)
                    .handle(newAloEmitter(messageCreator.compose(Alo::get)))
                    .transform(senderMessages -> sender.send(senderMessages, queueUrl))
                    .map(result -> result.correlationMetadata().map(result::replaceCorrelationMetadata));
        }

        public void close() {
            sender.close();
        }

        private <R> BiConsumer<Alo<R>, SynchronousSink<SqsSenderMessage<Alo<R>>>> newAloEmitter(
                Function<Alo<R>, SqsMessage<T>> aloToSqsMessage) {
            return (alo, sink) -> alo.runInContext(() -> sink.next(toSenderMessage(alo, aloToSqsMessage)));
        }

        private <R> SqsSenderMessage<R> toSenderMessage(R data, Function<R, SqsMessage<T>> dataToSqsMessage) {
            SqsMessage<T> sqsMessage = dataToSqsMessage.apply(data);
            return SqsSenderMessage.<R>newBuilder()
                    .messageDeduplicationId(sqsMessage.messageDeduplicationId().orElse(null))
                    .messageGroupId(sqsMessage.messageGroupId().orElse(null))
                    .messageAttributes(sqsMessage.messageAttributes())
                    .messageSystemAttributes(sqsMessage.messageSystemAttributes())
                    .body(bodySerializer.serialize(sqsMessage.body()))
                    .delaySeconds(sqsMessage.senderDelaySeconds().orElse(null))
                    .correlationMetadata(data)
                    .build();
        }
    }
}
