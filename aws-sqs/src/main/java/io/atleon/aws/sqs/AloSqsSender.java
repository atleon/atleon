package io.atleon.aws.sqs;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import io.atleon.core.SenderResult;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.SynchronousSink;

import java.io.Closeable;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A reactive sender of {@link Alo} data to SQS queues, with forwarding methods for non-Alo items.
 * <p>
 * At most one instance of a {@link SqsSender} is kept and can be closed upon invoking
 * {@link AloSqsSender#close()}. However, if after closing, more sent Publishers are subscribed to,
 * a new Sender instance will be created and cached.
 *
 * @param <T> The deserialized type of SQS Message bodies
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
        this.futureResources = configSource.create()
            .map(SendResources::<T>fromConfig)
            .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), SendResources::close);
    }

    public static <T> AloSqsSender<T> from(SqsConfigSource configSource) {
        return new AloSqsSender<>(configSource);
    }

    public Flux<SqsSenderResult<T>> sendBodies(Publisher<T> bodies, SqsMessageCreator<T> messageCreator, String queueUrl) {
        return futureResources.flatMapMany(resources -> resources.send(bodies, messageCreator, queueUrl));
    }

    public Mono<SqsSenderResult<SqsMessage<T>>> sendMessage(SqsMessage<T> message, String queueUrl) {
        return futureResources.flatMap(resources -> resources.send(message, queueUrl));
    }

    public Flux<SqsSenderResult<SqsMessage<T>>> sendMessages(Publisher<SqsMessage<T>> messages, String queueUrl) {
        return futureResources.flatMapMany(resources -> resources.send(messages, Function.identity(), queueUrl));
    }

    public Function<Publisher<Alo<T>>, AloFlux<SqsSenderResult<T>>> sendAloBodies(
        SqsMessageCreator<T> messageCreator,
        String queueUrl
    ) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator, queueUrl);
    }

    public AloFlux<SqsSenderResult<T>> sendAloBodies(
        Publisher<Alo<T>> aloBodies,
        SqsMessageCreator<T> messageCreator,
        String queueUrl
    ) {
        return futureResources.flatMapMany(resources -> resources.sendAlos(aloBodies, messageCreator, queueUrl))
            .as(AloFlux::wrap)
            .processFailure(SenderResult::isFailure, SenderResult::toError);
    }

    public Function<Publisher<Alo<SqsMessage<T>>>, AloFlux<SqsSenderResult<SqsMessage<T>>>> sendAloMessages(
        String queueUrl
    ) {
        return aloMessages -> sendAloMessages(aloMessages, queueUrl);
    }

    public AloFlux<SqsSenderResult<SqsMessage<T>>> sendAloMessages(
        Publisher<Alo<SqsMessage<T>>> aloMessages,
        String queueUrl
    ) {
        return futureResources.flatMapMany(resources -> resources.sendAlos(aloMessages, Function.identity(), queueUrl))
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
                .batchDuration(config.loadDuration(BATCH_DURATION_CONFIG).orElse(SqsSenderOptions.DEFAULT_BATCH_DURATION))
                .batchPrefetch(config.loadInt(BATCH_PREFETCH_CONFIG).orElse(SqsSenderOptions.DEFAULT_BATCH_PREFETCH))
                .maxRequestsInFlight(config.loadInt(MAX_REQUESTS_IN_FLIGHT_CONFIG).orElse(SqsSenderOptions.DEFAULT_MAX_REQUESTS_IN_FLIGHT))
                .build();
            return new SendResources<T>(
                SqsSender.create(options),
                config.loadConfiguredOrThrow(BODY_SERIALIZER_CONFIG, BodySerializer.class)
            );
        }

        public Mono<SqsSenderResult<SqsMessage<T>>> send(SqsMessage<T> message, String queueUrl) {
            return sender.send(toSenderMessage(message, Function.identity()), queueUrl);
        }

        public <R> Flux<SqsSenderResult<R>> send(
            Publisher<R> items,
            Function<R, SqsMessage<T>> messageCreator,
            String queueUrl
        ) {
            return Flux.from(items)
                .map(item -> toSenderMessage(item, messageCreator))
                .transform(senderMessages -> sender.send(senderMessages, queueUrl));
        }

        public <R> Flux<Alo<SqsSenderResult<R>>> sendAlos(
            Publisher<Alo<R>> alos,
            Function<R, SqsMessage<T>> messageCreator,
            String queueUrl
        ) {
            return AloFlux.toFlux(alos)
                .handle(newAloEmitter(messageCreator.compose(Alo::get)))
                .transform(senderMessages -> sender.send(senderMessages, queueUrl))
                .map(result -> result.correlationMetadata().map(result::replaceCorrelationMetadata));
        }

        public void close() {
            sender.close();
        }

        private <R> BiConsumer<Alo<R>, SynchronousSink<SqsSenderMessage<Alo<R>>>> newAloEmitter(
            Function<Alo<R>, SqsMessage<T>> aloToSqsMessage
        ) {
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
