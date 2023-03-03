package io.atleon.aws.sns;

import io.atleon.core.Alo;
import io.atleon.core.AloFlux;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.Closeable;
import java.util.function.Function;

/**
 * A reactive sender of {@link Alo} data to SNS topics, with forwarding methods for non-Alo items.
 * <P>
 * At most one instance of an {@link SnsSender} is kept and can be closed upon invoking
 * {@link AloSnsSender#close()}. However, if after closing, more sent Publishers are subscribed to,
 * a new Client instance will be created and cached.
 *
 * @param <T> The deserialized type of SNS Message bodies
 */
public class AloSnsSender<T> implements Closeable {

    /**
     * Prefix used on all AloSnsSender-specific configurations.
     */
    public static final String CONFIG_PREFIX = "sns.sender.";

    /**
     * Qualified class name of {@link BodySerializer} implementation used to serialize Message
     * bodies.
     */
    public static final String BODY_SERIALIZER_CONFIG = CONFIG_PREFIX + "body.serializer";

    /**
     * The max number of Messages to send in each SNS batch send request. Defaults to 1, meaning
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
     * Configures the maximum number of SNS Requests in flight per sent Publisher. When batching is
     * disabled, this equates to the maximum number of Messages concurrently being sent. When
     * batching is enabled, this equates to the maximum number batches concurrently being sent.
     */
    public static final String MAX_REQUESTS_IN_FLIGHT_CONFIG = CONFIG_PREFIX + "max.requests.in.flight";

    private static final Logger LOGGER = LoggerFactory.getLogger(AloSnsSender.class);

    private final Mono<Resources<T>> futureResources;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private AloSnsSender(SnsConfigSource configSource) {
        this.futureResources = configSource.create()
            .map(Resources::<T>fromConfig)
            .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), Resources::close);
    }

    public static <T> AloSnsSender<T> from(SnsConfigSource configSource) {
        return new AloSnsSender<>(configSource);
    }

    public Function<Publisher<T>, Flux<SnsSenderResult<T>>> sendBodies(
        SnsMessageCreator<T> messageCreator,
        String topicArn
    ) {
        return bodies -> sendBodies(bodies, messageCreator, topicArn);
    }

    public Flux<SnsSenderResult<T>> sendBodies(Publisher<T> bodies, SnsMessageCreator<T> messageCreator, String topicArn) {
        return futureResources.flatMapMany(resources -> resources.send(bodies, messageCreator, topicArn));
    }

    public Mono<SnsSenderResult<SnsMessage<T>>> sendMessage(SnsMessage<T> message, SnsAddress address) {
        return futureResources.flatMap(resources -> resources.send(message, address));
    }

    public Flux<SnsSenderResult<SnsMessage<T>>> sendMessages(Publisher<SnsMessage<T>> messages, String topicArn) {
        return futureResources.flatMapMany(resources -> resources.send(messages, Function.identity(), topicArn));
    }

    public Function<Publisher<Alo<T>>, AloFlux<SnsSenderResult<T>>> sendAloBodies(
        SnsMessageCreator<T> messageCreator,
        String topicArn
    ) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator, topicArn);
    }

    public AloFlux<SnsSenderResult<T>> sendAloBodies(
        Publisher<Alo<T>> aloBodies,
        SnsMessageCreator<T> messageCreator,
        String topicArn
    ) {
        return futureResources
            .flatMapMany(resources -> resources.sendAlos(aloBodies, messageCreator, topicArn))
            .as(AloFlux::wrap);
    }

    public Function<Publisher<Alo<SnsMessage<T>>>, AloFlux<SnsSenderResult<SnsMessage<T>>>> sendAloMessages(
        String topicArn
    ) {
        return aloMessages -> sendAloMessages(aloMessages, topicArn);
    }

    public AloFlux<SnsSenderResult<SnsMessage<T>>> sendAloMessages(
        Publisher<Alo<SnsMessage<T>>> aloMessages,
        String topicArn
    ) {
        return futureResources
            .flatMapMany(resources -> resources.sendAlos(aloMessages, Function.identity(), topicArn))
            .as(AloFlux::wrap);
    }

    /**
     * Closes this sender and logs the provided reason.
     *
     * @param reason The reason this sender is being closed
     */
    public void close(Object reason) {
        LOGGER.info("Closing AloSnsSender due to reason={}", reason);
        close();
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private static final class Resources<T> {

        private final SnsSender sender;

        private final BodySerializer<T> bodySerializer;

        private Resources(SnsSender sender, BodySerializer<T> bodySerializer) {
            this.sender = sender;
            this.bodySerializer = bodySerializer;
        }

        public static <T> Resources<T> fromConfig(SnsConfig config) {
            SnsSenderOptions options = SnsSenderOptions.newBuilder(config::buildClient)
                .batchSize(config.loadInt(BATCH_SIZE_CONFIG, SnsSenderOptions.DEFAULT_BATCH_SIZE))
                .batchDuration(config.loadDuration(BATCH_DURATION_CONFIG, SnsSenderOptions.DEFAULT_BATCH_DURATION))
                .batchPrefetch(config.loadInt(BATCH_PREFETCH_CONFIG, SnsSenderOptions.DEFAULT_BATCH_PREFETCH))
                .maxRequestsInFlight(config.loadInt(MAX_REQUESTS_IN_FLIGHT_CONFIG, SnsSenderOptions.DEFAULT_MAX_REQUESTS_IN_FLIGHT))
                .build();
            return new Resources<>(SnsSender.create(options), config.loadConfiguredOrThrow(BODY_SERIALIZER_CONFIG));
        }

        public  Mono<SnsSenderResult<SnsMessage<T>>> send(SnsMessage<T> message, SnsAddress address) {
            return sender.send(toSenderMessage(message, Function.identity()), address);
        }

        public <R> Flux<SnsSenderResult<R>> send(
            Publisher<R> items,
            Function<R, SnsMessage<T>> messageCreator,
            String topicArn
        ) {
            return Flux.from(items)
                .map(item -> toSenderMessage(item, messageCreator))
                .transform(senderMessages -> sender.send(senderMessages, topicArn));
        }

        public <R> Flux<Alo<SnsSenderResult<R>>> sendAlos(
            Publisher<Alo<R>> alos,
            Function<R, SnsMessage<T>> messageCreator,
            String topicArn
        ) {
            return AloFlux.toFlux(alos)
                .map(alo -> alo.supplyInContext(() -> toSenderMessage(alo, messageCreator.compose(Alo::get))))
                .transform(senderMessages -> sender.send(senderMessages, topicArn))
                .map(this::toAloOfMessageResult);
        }

        public void close() {
            sender.close();
        }

        private <R> SnsSenderMessage<R> toSenderMessage(R data, Function<R, SnsMessage<T>> dataToSnsMessage) {
            SnsMessage<T> snsMessage = dataToSnsMessage.apply(data);
            return SnsSenderMessage.<R>newBuilder()
                .messageDeduplicationId(snsMessage.messageDeduplicationId().orElse(null))
                .messageGroupId(snsMessage.messageGroupId().orElse(null))
                .messageAttributes(snsMessage.messageAttributes())
                .messageStructure(snsMessage.messageStructure().orElse(null))
                .subject(snsMessage.subject().orElse(null))
                .body(bodySerializer.serialize(snsMessage.body()))
                .correlationMetadata(data)
                .build();
        }

        private <R> Alo<SnsSenderResult<R>> toAloOfMessageResult(SnsSenderResult<Alo<R>> messageResultOfAlo) {
            Alo<R> alo = messageResultOfAlo.correlationMetadata();
            return alo.<SnsSenderResult<R>>propagator()
                .create(messageResultOfAlo.mapCorrelationMetadata(Alo::get), alo.getAcknowledger(), alo.getNacknowledger());
        }
    }
}
