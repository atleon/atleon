package io.atleon.aws.sqs;

import io.atleon.core.Batcher;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResultEntry;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A low-level sender of {@link SqsMessage}s. Sent Messages contain the raw String body payload and
 * may reference correlated metadata that is propagated downstream with the Result of sending any
 * given Message.
 * <P>
 * At most one instance of an {@link SqsAsyncClient} is kept and can be closed upon invoking
 * {@link SqsSender#close()}. However, if after closing, more sent Publishers are subscribed to, a
 * new Client instance will be created and cached.
 */
public final class SqsSender implements Closeable {

    private static final Retry DEFAULT_RETRY = Retry.backoff(3, Duration.ofMillis(10));

    private final Mono<SqsAsyncClient> futureClient;

    private final Batcher batcher;

    private final int maxRequestsInFlight;

    private final Sinks.Many<Long> closeSink = Sinks.many().multicast().directBestEffort();

    private SqsSender(SqsSenderOptions options) {
        this.futureClient = Mono.fromSupplier(options::createClient)
            .cacheInvalidateWhen(client -> closeSink.asFlux().next().then(), SqsAsyncClient::close);
        this.batcher = Batcher.create(options.batchSize(), options.batchDuration(), options.batchPrefetch());
        this.maxRequestsInFlight = options.maxRequestsInFlight();
    }

    /**
     * Creates a reactive SQS sender with the specified configuration options.
     */
    public static SqsSender create(SqsSenderOptions options) {
        return new SqsSender(options);
    }

    /**
     * Sends a single {@link SqsSenderMessage} to the provided queue URL
     *
     * @param message  A message to send
     * @param queueUrl The URL of the queue to which the message will be sent
     * @param <C> The type of correlated metadata associated with the sent message
     * @return A Publisher of the result of sending the message
     */
    public <C> Mono<SqsSenderResult<C>> send(SqsSenderMessage<C> message, String queueUrl) {
        return futureClient.flatMapMany(client -> send(client, Collections.singletonList(message), queueUrl)).next();
    }

    /**
     * Sends a sequence of {@link SqsSenderMessage}s to the provided SQS queue URL
     * <p>
     * When maxRequestsInFlight is {@literal <=} 1, results will be published in the same order
     * that their corresponding messages are sent. If maxRequestsInFlight is {@literal >} 1, it is
     * possible that results may be emitted out of order as concurrent requests may complete with
     * differing latencies.
     *
     * @param messages A Publisher of SqsSenderMessages to send
     * @param queueUrl The URL of the queue to which the messages will be sent
     * @param <C> The type of correlated metadata associated with the sent messages
     * @return A Publisher of the results of sending each message
     */
    public <C> Flux<SqsSenderResult<C>> send(Publisher<SqsSenderMessage<C>> messages, String queueUrl) {
        return futureClient.flatMapMany(client ->
            batcher.applyMapping(messages, batch -> send(client, batch, queueUrl), maxRequestsInFlight)
        );
    }

    @Override
    public void close() {
        closeSink.tryEmitNext(System.currentTimeMillis());
    }

    private <C> Flux<SqsSenderResult<C>> send(SqsAsyncClient client, List<SqsSenderMessage<C>> messages, String queueUrl) {
        SendMessageBatchRequest request = SendMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(messages.stream().map(this::createBatchRequestEntry).collect(Collectors.toList()))
            .build();
        Map<String, C> correlationMetadataByRequestId = messages.stream()
            .filter(message -> message.correlationMetadata() != null)
            .collect(Collectors.toMap(SqsSenderMessage::requestId, SqsSenderMessage::correlationMetadata));
        return Mono.fromFuture(() -> client.sendMessageBatch(request))
            .retryWhen(DEFAULT_RETRY)
            .flatMapIterable(response -> createResults(response, correlationMetadataByRequestId));
    }

    private <C> SendMessageBatchRequestEntry createBatchRequestEntry(SqsSenderMessage<C> message) {
        return SendMessageBatchRequestEntry.builder()
            .id(message.requestId())
            .messageDeduplicationId(message.messageDeduplicationId().orElse(null))
            .messageGroupId(message.messageGroupId().orElse(null))
            .messageAttributes(message.messageAttributes())
            .messageSystemAttributesWithStrings(message.messageSystemAttributes())
            .messageBody(message.body())
            .delaySeconds(message.senderDelaySeconds().orElse(null))
            .build();
    }

    private <C> List<SqsSenderResult<C>> createResults(
        SendMessageBatchResponse response,
        Map<String, C> correlationMetadataByRequestId
    ) {
        Stream<SqsSenderResult<C>> failures = response.failed().stream()
            .map(entry -> createFailureResult(entry, correlationMetadataByRequestId.get(entry.id())));
        Stream<SqsSenderResult<C>> successes = response.successful().stream()
            .map(entry -> createSuccessResult(entry, correlationMetadataByRequestId.get(entry.id())));
        return Stream.concat(failures, successes).collect(Collectors.toList());
    }

    private <C> SqsSenderResult<C> createFailureResult(BatchResultErrorEntry entry, C correlationMetadata) {
        Throwable error = new MessageSendFailedException(entry.code(), entry.message());
        return SqsSenderResult.failure(entry.id(), error, correlationMetadata);
    }

    private <C> SqsSenderResult<C> createSuccessResult(SendMessageBatchResultEntry entry, C correlationMetadata) {
        return SqsSenderResult.success(entry.id(), entry.messageId(), entry.sequenceNumber(), correlationMetadata);
    }

    public static final class MessageSendFailedException extends RuntimeException {

        private final String code;

        private final String message;

        public MessageSendFailedException(String code, String message) {
            super(String.format("Sending message failed with code=%s: %s", code, message));
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
