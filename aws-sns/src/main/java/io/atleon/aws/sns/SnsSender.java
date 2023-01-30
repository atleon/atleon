package io.atleon.aws.sns;

import io.atleon.core.Batcher;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchRequest;
import software.amazon.awssdk.services.sns.model.PublishBatchRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishBatchResponse;
import software.amazon.awssdk.services.sns.model.PublishBatchResultEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A low-level sender of {@link SnsMessage}s. Sent Messages contain the raw String body payload and
 * may reference correlated metadata that is propagated downstream with the Result of sending any
 * given Message.
 * <p>
 * At most one instance of an {@link SnsAsyncClient} is kept and can be closed upon invoking
 * {@link SnsSender#close()}. However, if after closing, more sent Publishers are subscribed to, a
 * new Client instance will be created and cached.
 */
public final class SnsSender implements Closeable {

    private static final Retry DEFAULT_RETRY = Retry.backoff(3, Duration.ofMillis(10));

    private final Mono<SnsAsyncClient> futureClient;

    private final Batcher batcher;

    private final int maxRequestsInFlight;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private SnsSender(SnsSenderOptions options) {
        this.futureClient = Mono.fromSupplier(options::createClient)
            .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), SnsAsyncClient::close);
        this.batcher = Batcher.create(options.batchSize(), options.batchDuration(), options.batchPrefetch());
        this.maxRequestsInFlight = options.maxRequestsInFlight();
    }

    /**
     * Creates a reactive SNS sender with the specified configuration options.
     */
    public static SnsSender create(SnsSenderOptions options) {
        return new SnsSender(options);
    }

    /**
     * Sends a single {@link SnsSenderMessage} to the provided {@link SnsAddress}
     *
     * @param message A message to send
     * @param address The address to which the published message will be sent
     * @param <C> The type of correlated metadata associated with the sent message
     * @return A Publisher of the result of sending the message
     */
    public <C> Mono<SnsSenderResult<C>> send(SnsSenderMessage<C> message, SnsAddress address) {
        return futureClient.flatMap(client -> send(client, message, address));
    }

    /**
     * Sends a sequence of {@link SnsSenderMessage}s to the provided SNS topic ARN
     * <p>
     * When maxRequestsInFlight is {@literal <=} 1, results will be published in the same order
     * that their corresponding messages are sent. If maxRequestsInFlight is {@literal >} 1, it is
     * possible that results may be emitted out of order as concurrent requests may complete with
     * differing latencies.
     *
     * @param messages A Publisher of SnsSenderMessages to send
     * @param topicArn The ARN of the topic to which the published messages will be sent
     * @param <C> The type of correlated metadata associated with the sent messages
     * @return A Publisher of the results of sending each message
     */
    public <C> Flux<SnsSenderResult<C>> send(Publisher<SnsSenderMessage<C>> messages, String topicArn) {
        return futureClient.flatMapMany(client ->
            batcher.applyMapping(messages, batch -> send(client, batch, topicArn), maxRequestsInFlight)
        );
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private <C> Mono<SnsSenderResult<C>> send(SnsAsyncClient client, SnsSenderMessage<C> message, SnsAddress address) {
        PublishRequest request = createPublishRequest(message, address);
        String requestId = message.requestId();
        C correlationMetadata = message.correlationMetadata();
        return Mono.fromFuture(() -> client.publish(request))
            .retryWhen(DEFAULT_RETRY)
            .map(response -> createSuccessResult(requestId, response, correlationMetadata))
            .onErrorResume(error -> Mono.just(createFailureResult(requestId, error, correlationMetadata)));
    }

    private <C> Flux<SnsSenderResult<C>> send(SnsAsyncClient client, List<SnsSenderMessage<C>> messages, String topicArn) {
        PublishBatchRequest request = PublishBatchRequest.builder()
            .topicArn(topicArn)
            .publishBatchRequestEntries(messages.stream().map(this::createBatchRequestEntry).collect(Collectors.toList()))
            .build();
        Map<String, C> correlationMetadataByRequestId = messages.stream()
            .filter(message -> message.correlationMetadata() != null)
            .collect(Collectors.toMap(SnsSenderMessage::requestId, SnsSenderMessage::correlationMetadata));
        return Mono.fromFuture(() -> client.publishBatch(request))
            .retryWhen(DEFAULT_RETRY)
            .flatMapIterable(response -> createResults(response, correlationMetadataByRequestId));
    }

    private <C> PublishRequest createPublishRequest(SnsSenderMessage<C> message, SnsAddress address) {
        return newPublishRequestBuilder(address)
            .messageDeduplicationId(message.messageDeduplicationId().orElse(null))
            .messageGroupId(message.messageGroupId().orElse(null))
            .messageAttributes(message.messageAttributes())
            .messageStructure(message.messageStructure().orElse(null))
            .subject(message.subject().orElse(null))
            .message(message.body())
            .build();
    }

    private PublishRequest.Builder newPublishRequestBuilder(SnsAddress address) {
        switch (address.type()) {
            case TOPIC_ARN:
                return PublishRequest.builder().topicArn(address.value());
            case TARGET_ARN:
                return PublishRequest.builder().targetArn(address.value());
            case PHONE_NUMBER:
                return PublishRequest.builder().phoneNumber(address.value());
            default:
                throw new UnsupportedOperationException("Publishing not supported for addressType=" + address.type());
        }
    }

    private <C> PublishBatchRequestEntry createBatchRequestEntry(SnsSenderMessage<C> message) {
        return PublishBatchRequestEntry.builder()
            .id(message.requestId())
            .messageDeduplicationId(message.messageDeduplicationId().orElse(null))
            .messageGroupId(message.messageGroupId().orElse(null))
            .messageAttributes(message.messageAttributes())
            .messageStructure(message.messageStructure().orElse(null))
            .subject(message.subject().orElse(null))
            .message(message.body())
            .build();
    }

    private <C> List<SnsSenderResult<C>> createResults(
        PublishBatchResponse response,
        Map<String, C> correlationMetadataByRequestId
    ) {
        Stream<SnsSenderResult<C>> failures = response.failed().stream()
            .map(entry -> createFailureResult(entry, correlationMetadataByRequestId.get(entry.id())));
        Stream<SnsSenderResult<C>> successes = response.successful().stream()
            .map(entry -> createSuccessResult(entry, correlationMetadataByRequestId.get(entry.id())));
        return Stream.concat(failures, successes).collect(Collectors.toList());
    }

    private <C> SnsSenderResult<C> createFailureResult(String requestId, Throwable error, C correlationMetadata) {
        return SnsSenderResult.failure(requestId, error, correlationMetadata);
    }

    private <C> SnsSenderResult<C> createFailureResult(BatchResultErrorEntry entry, C correlationMetadata) {
        Throwable error = new MessagePublishFailedException(entry.code(), entry.message());
        return SnsSenderResult.failure(entry.id(), error, correlationMetadata);
    }

    private <C> SnsSenderResult<C> createSuccessResult(String requestId, PublishResponse response, C correlationMetadata) {
        return SnsSenderResult.success(requestId, response.messageId(), response.sequenceNumber(), correlationMetadata);
    }

    private <C> SnsSenderResult<C> createSuccessResult(PublishBatchResultEntry entry, C correlationMetadata) {
        return SnsSenderResult.success(entry.id(), entry.messageId(), entry.sequenceNumber(), correlationMetadata);
    }

    public static final class MessagePublishFailedException extends RuntimeException {

        private final String code;

        private final String message;

        public MessagePublishFailedException(String code, String message) {
            super(String.format("Publishing message failed with code=%s: %s", code, message));
            this.code = code;
            this.message = message;
        }

        public String code() {
            return code;
        }

        public String message() {
            return message;
        }
    }
}
