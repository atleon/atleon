package io.atleon.aws.sns;

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
 * A reactive sender of {@link Alo} data to SNS topics, with forwarding methods for non-Alo items.
 * <p>
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

    private final Mono<SendResources<T>> futureResources;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private AloSnsSender(SnsConfigSource configSource) {
        this.futureResources = configSource
                .create()
                .map(SendResources::<T>fromConfig)
                .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), SendResources::close);
    }

    /**
     * Alias for {@link #create(SnsConfigSource)}. Will be deprecated in future release.
     */
    public static <T> AloSnsSender<T> from(SnsConfigSource configSource) {
        return create(configSource);
    }

    /**
     * Creates a new AloSnsSender from the provided {@link SnsConfigSource}
     *
     * @param configSource The reactive source of {@link SnsConfig}
     * @param <T>          The type of messages bodies sent by this sender
     * @return A new AloSnsSender
     */
    public static <T> AloSnsSender<T> create(SnsConfigSource configSource) {
        return new AloSnsSender<>(configSource);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of SNS message bodies
     * to a Publisher of results of* sending each message body. See
     * {@link #sendBodies(Publisher, SnsMessageCreator, String)} for further information.
     *
     * @param messageCreator A factory that creates {@link SnsMessage}s from message bodies
     * @param topicArn       ARN of the topic to send messages to
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<T>, Flux<SnsSenderResult<T>>> sendBodies(
            SnsMessageCreator<T> messageCreator, String topicArn) {
        return bodies -> sendBodies(bodies, messageCreator, topicArn);
    }

    /**
     * Sends a sequence of message bodies to be populated in {@link SnsMessage}s to the specified
     * topic ARN.
     * <p>
     * The output of each sent message body is an {@link SnsSenderResult} containing the sent
     * value.
     *
     * @param bodies         A Publisher of SNS message bodies
     * @param messageCreator A factory that creates {@link SnsMessage}s from message bodies
     * @param topicArn       ARN of the topic to send messages to
     * @return a Publisher of the results of each sent message
     */
    public Flux<SnsSenderResult<T>> sendBodies(
            Publisher<T> bodies, SnsMessageCreator<T> messageCreator, String topicArn) {
        return futureResources.flatMapMany(resources -> resources.send(bodies, messageCreator, topicArn));
    }

    /**
     * Send a single {@link SnsMessage}
     *
     * @param message A message to send
     * @param address Address to send the message to
     * @return A Publisher of the result of sending the message
     */
    public Mono<SnsSenderResult<SnsMessage<T>>> sendMessage(SnsMessage<T> message, SnsAddress address) {
        return futureResources.flatMap(resources -> resources.send(message, address));
    }

    /**
     * Sends a sequence of {@link SnsMessage}s
     * <p>
     * The output of each sent message is an {@link SnsSenderResult} containing the sent
     * message.
     *
     * @param messages A Publisher of messages to send
     * @param topicArn ARN of the topic to send messages to
     * @return A Publisher of items referencing the result of each sent message
     */
    public Flux<SnsSenderResult<SnsMessage<T>>> sendMessages(Publisher<SnsMessage<T>> messages, String topicArn) {
        return futureResources.flatMapMany(resources -> resources.send(messages, Function.identity(), topicArn));
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing SNS message bodies to a Publisher of Alo items referencing the result of
     * sending each message body. See {@link #sendAloBodies(Publisher, SnsMessageCreator, String)}
     * for further information.
     *
     * @param messageCreator A factory that creates {@link SnsMessage}s from message bodies
     * @param topicArn       ARN of the topic to send messages to
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<T>>, AloFlux<SnsSenderResult<T>>> sendAloBodies(
            SnsMessageCreator<T> messageCreator, String topicArn) {
        return aloBodies -> sendAloBodies(aloBodies, messageCreator, topicArn);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing message bodies to be populated in
     * {@link SnsMessage}s to the specified topic ARN.
     * <p>
     * The output of each sent message body is an {@link SnsSenderResult} containing the sent
     * value. Each emitted item is an {@link Alo} item referencing an {@link SnsSenderResult}
     * and must be acknowledged or nacknowledged such that its processing can be marked complete at
     * the origin of the message.
     *
     * @param aloBodies      A Publisher of Alo items referencing SNS message bodies
     * @param messageCreator A factory that creates {@link SnsMessage}s from message bodies
     * @param topicArn       ARN of the topic to send messages to
     * @return a Publisher of Alo items referencing the result of each sent message
     */
    public AloFlux<SnsSenderResult<T>> sendAloBodies(
            Publisher<Alo<T>> aloBodies, SnsMessageCreator<T> messageCreator, String topicArn) {
        return futureResources
                .flatMapMany(resources -> resources.sendAlos(aloBodies, messageCreator, topicArn))
                .as(AloFlux::wrap)
                .processFailure(SenderResult::isFailure, SenderResult::toError);
    }

    /**
     * Creates a {@link Function} that can be used to transform a Publisher of {@link Alo} items
     * referencing SNS messages to a Publisher of Alo items referencing the result of sending each
     * message. See {@link #sendAloMessages(Publisher, String)} for further information.
     *
     * @param topicArn ARN of the topic to send messages to
     * @return A {@link Function} useful for Publisher transformations
     */
    public Function<Publisher<Alo<SnsMessage<T>>>, AloFlux<SnsSenderResult<SnsMessage<T>>>> sendAloMessages(
            String topicArn) {
        return aloMessages -> sendAloMessages(aloMessages, topicArn);
    }

    /**
     * Sends a sequence of {@link Alo} items referencing {@link SnsMessage}s
     * <p>
     * The output of each sent message is an {@link SnsSenderResult} containing the sent
     * message. Each emitted item is an {@link Alo} item referencing a {@link SnsSenderResult}
     * and must be acknowledged or nacknowledged such that its processing can be marked complete at
     * the origin of the message.
     *
     * @param aloMessages A Publisher of Alo items referencing messages to send
     * @param topicArn    ARN of the topic to send messages to
     * @return A Publisher of Alo items referencing the result of each sent message
     */
    public AloFlux<SnsSenderResult<SnsMessage<T>>> sendAloMessages(
            Publisher<Alo<SnsMessage<T>>> aloMessages, String topicArn) {
        return futureResources
                .flatMapMany(resources -> resources.sendAlos(aloMessages, Function.identity(), topicArn))
                .as(AloFlux::wrap)
                .processFailure(SenderResult::isFailure, SenderResult::toError);
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

    private static final class SendResources<T> {

        private final SnsSender sender;

        private final BodySerializer<T> bodySerializer;

        private SendResources(SnsSender sender, BodySerializer<T> bodySerializer) {
            this.sender = sender;
            this.bodySerializer = bodySerializer;
        }

        public static <T> SendResources<T> fromConfig(SnsConfig config) {
            SnsSenderOptions options = SnsSenderOptions.newBuilder(config::buildClient)
                    .batchSize(config.loadInt(BATCH_SIZE_CONFIG).orElse(SnsSenderOptions.DEFAULT_BATCH_SIZE))
                    .batchDuration(
                            config.loadDuration(BATCH_DURATION_CONFIG).orElse(SnsSenderOptions.DEFAULT_BATCH_DURATION))
                    .batchPrefetch(
                            config.loadInt(BATCH_PREFETCH_CONFIG).orElse(SnsSenderOptions.DEFAULT_BATCH_PREFETCH))
                    .maxRequestsInFlight(config.loadInt(MAX_REQUESTS_IN_FLIGHT_CONFIG)
                            .orElse(SnsSenderOptions.DEFAULT_MAX_REQUESTS_IN_FLIGHT))
                    .build();
            return new SendResources<T>(
                    SnsSender.create(options),
                    config.loadConfiguredOrThrow(BODY_SERIALIZER_CONFIG, BodySerializer.class));
        }

        public Mono<SnsSenderResult<SnsMessage<T>>> send(SnsMessage<T> message, SnsAddress address) {
            return sender.send(toSenderMessage(message, Function.identity()), address);
        }

        public <R> Flux<SnsSenderResult<R>> send(
                Publisher<R> items, Function<R, SnsMessage<T>> messageCreator, String topicArn) {
            return Flux.from(items)
                    .map(item -> toSenderMessage(item, messageCreator))
                    .transform(senderMessages -> sender.send(senderMessages, topicArn));
        }

        public <R> Flux<Alo<SnsSenderResult<R>>> sendAlos(
                Publisher<Alo<R>> alos, Function<R, SnsMessage<T>> messageCreator, String topicArn) {
            return AloFlux.toFlux(alos)
                    .handle(newAloEmitter(messageCreator.compose(Alo::get)))
                    .transform(senderMessages -> sender.send(senderMessages, topicArn))
                    .map(result -> result.correlationMetadata().map(result::replaceCorrelationMetadata));
        }

        public void close() {
            sender.close();
        }

        private <R> BiConsumer<Alo<R>, SynchronousSink<SnsSenderMessage<Alo<R>>>> newAloEmitter(
                Function<Alo<R>, SnsMessage<T>> aloToSnsMessage) {
            return (alo, sink) -> alo.runInContext(() -> sink.next(toSenderMessage(alo, aloToSnsMessage)));
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
    }
}
