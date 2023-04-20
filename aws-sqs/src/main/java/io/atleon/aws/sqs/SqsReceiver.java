package io.atleon.aws.sqs;

import io.atleon.core.ReactivePhaser;
import io.atleon.core.SerialQueue;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A low-level receiver of {@link SqsMessage}s. Received messages contain the raw String body
 * payloads as received from requests to SQS. Each Subscription to Messages is backed by its own
 * {@link SqsAsyncClient} which is disposed/closed upon termination of the Subscription.
 */
public final class SqsReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqsReceiver.class);

    private static final Retry DEFAULT_RETRY = Retry.backoff(3, Duration.ofMillis(10));

    private static final ReceiveMessageResponse EMPTY_RECEIVE_MESSAGE_RESPONSE = ReceiveMessageResponse.builder().build();

    private final SqsReceiverOptions options;

    private SqsReceiver(SqsReceiverOptions options) {
        this.options = options;
    }

    /**
     * Creates a reactive SQS receiver with the specified configuration options.
     */
    public static SqsReceiver create(SqsReceiverOptions options) {
        return new SqsReceiver(options);
    }

    /**
     * Receive {@link SqsMessage}s where each Message's deletion and visibility must be explicitly
     * handled. If a received Message is not deleted or has its visibility managed before the
     * visibility timeout lapses, the Message may be received again, and operations on the original
     * Message (using its original receipt handle) may result in errors indicating the Message
     * could not be found.
     *
     * @param queueUrl The URL of the Queue to receive messages from
     * @return Flux of inbound Messages whose visibility and deletion must be manually handled
     */
    public Flux<SqsReceiverMessage> receiveManual(String queueUrl) {
        return Flux.from(subscriber -> subscriber.onSubscribe(new Poller(options::createClient, queueUrl, subscriber)));
    }

    public static final class BatchRequestFailedException extends RuntimeException {

        private BatchRequestFailedException(String type, List<BatchResultErrorEntry> entries) {
            super(String.format("Batch request failed! type=%s errors=%s", type, entries));
        }
    }

    private final class Poller implements Subscription {

        private final SqsAsyncClient client;

        private final String queueUrl;

        private final Subscriber<? super SqsReceiverMessage> subscriber;

        private final ReactivePhaser executionPhaser = new ReactivePhaser(1);

        private final AtomicLong requestOutstanding = new AtomicLong(0);

        private final AtomicBoolean receptionPending = new AtomicBoolean(false);

        private final AtomicBoolean done = new AtomicBoolean(false);

        // Receipt handles that have been emitted, but not finished processing
        private final Set<String> inProcessReceiptHandles = Collections.newSetFromMap(new ConcurrentHashMap<>());

        // Receipt handles that have been emitted, but not deleted nor had their visibility reset
        private final Set<String> inFlightReceiptHandles = Collections.newSetFromMap(new ConcurrentHashMap<>());

        private final Sinks.Many<String> receiptHandlesToDelete = Sinks.unsafe().many().unicast().onBackpressureError();

        private final SerialQueue<String> receiptHandlesToDeleteQueue = SerialQueue.onEmitNext(receiptHandlesToDelete);

        public Poller(Supplier<SqsAsyncClient> client, String queueUrl, Subscriber<? super SqsReceiverMessage> subscriber) {
            this.client = client.get();
            this.queueUrl = queueUrl;
            this.subscriber = subscriber;
            this.receiptHandlesToDelete.asFlux()
                .doOnComplete(executionPhaser::register) // Avoid race condition with batch Scheduler and disposing
                .transform(receiptHandles -> batch(receiptHandles, options.deleteBatchSize(), options.deleteInterval()))
                .doAfterTerminate(executionPhaser::arriveAndDeregister)
                .subscribe(this::deleteMessages, this::doError);
        }

        @Override
        public void request(long n) {
            requestOutstanding.addAndGet(n);
            maybeScheduleMessageReception();
        }

        @Override
        public void cancel() {
            dispose().subscribe();
        }

        private Mono<Boolean> dispose() {
            return Mono.fromSupplier(() -> done.compareAndSet(false, true))
                .flatMap(shouldDispose -> shouldDispose ? doDispose().thenReturn(true) : Mono.just(false));
        }

        private Mono<?> doDispose() {
            return executionPhaser.arriveAndAwaitAdvanceReactively()
                .then(Mono.fromRunnable(receiptHandlesToDelete::tryEmitComplete))
                .then(Mono.defer(() -> createChangeMessageVisibilities(inProcessReceiptHandles, Duration.ZERO, __ -> true)))
                .then(executionPhaser.arriveAndAwaitAdvanceReactively())
                .timeout(options.closeTimeout())
                .doFinally(__ -> client.close())
                .doOnError(error -> LOGGER.error("Encountered error while disposing SQS Poller", error))
                .onErrorResume(error -> Mono.empty());
        }

        private void maybeScheduleMessageReception() {
            int maxNumberOfMessagesToRequest = calculateMaxNumberOfMessagesToRequest();
            if (maxNumberOfMessagesToRequest > 0 && !done.get() && receptionPending.compareAndSet(false, true)) {
                ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                    .receiveRequestAttemptId(UUID.randomUUID().toString())
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(maxNumberOfMessagesToRequest)
                    .messageAttributeNames(options.messageAttributesToRequest())
                    .attributeNamesWithStrings(options.messageSystemAttributesToRequest())
                    .waitTimeSeconds(options.waitTimeSecondsPerReception())
                    .visibilityTimeout(options.visibilityTimeoutSeconds())
                    .build();
                maybeExecute(SqsAsyncClient::receiveMessage, request, phase -> phase == 0)
                    .defaultIfEmpty(EMPTY_RECEIVE_MESSAGE_RESPONSE) // Should emit something so handler will run
                    .subscribe(this::handleMessagesReceived, this::handleMessagesReceivedError);
            }
        }

        private int calculateMaxNumberOfMessagesToRequest() {
            int remainingInFlightCapacity = options.maxInFlightPerSubscription() - inFlightReceiptHandles.size();
            int maxNumberOfMessagesToEmit = (int) Math.min(requestOutstanding.get(), remainingInFlightCapacity);
            return Math.min(options.maxMessagesPerReception(), maxNumberOfMessagesToEmit);
        }

        private void handleMessagesReceived(ReceiveMessageResponse response) {
            response.messages().forEach(this::emit);
            receptionPending.set(false);
            maybeScheduleMessageReception();
        }

        private void handleMessagesReceivedError(Throwable error) {
            doError(error);
            receptionPending.set(false);
        }

        private void emit(Message message) {
            String receiptHandle = message.receiptHandle();
            Runnable deleter = () -> {
                if (executionPhaser.register() == 0 && !done.get() && inProcessReceiptHandles.remove(receiptHandle)) {
                    receiptHandlesToDeleteQueue.addAndDrain(receiptHandle);
                }
                executionPhaser.arriveAndDeregister();
            };

            SqsMessageVisibilityChanger visibilityChanger = (timeout, stillInProcess) -> {
                if (executionPhaser.register() == 0 && !done.get()) {
                    if (stillInProcess && inProcessReceiptHandles.contains(receiptHandle)) {
                        maybeChangeMessageVisibility(receiptHandle, timeout);
                    } else if (!stillInProcess && inProcessReceiptHandles.remove(receiptHandle)) {
                        maybeChangeMessageVisibilityAndMarkNotInFlight(receiptHandle, timeout);
                    }
                }
                executionPhaser.arriveAndDeregister();
            };

            inFlightReceiptHandles.add(receiptHandle);
            inProcessReceiptHandles.add(receiptHandle);
            doNext(SqsReceiverMessage.create(message, deleter, visibilityChanger));
        }

        private void deleteMessages(Collection<String> receiptHandles) {
            if (receiptHandles.isEmpty()) return;
            List<DeleteMessageBatchRequestEntry> entries = receiptHandles.stream()
                .map(it -> DeleteMessageBatchRequestEntry.builder().id(newReceiptHandleId()).receiptHandle(it).build())
                .collect(Collectors.toList());
            DeleteMessageBatchRequest request = DeleteMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(entries)
                .build();
            maybeExecute(SqsAsyncClient::deleteMessageBatch, request, __ -> true)
                .subscribe(response -> handleMessagesDeleted(response, receiptHandles), this::doError);
        }

        private void handleMessagesDeleted(DeleteMessageBatchResponse response, Collection<String> receiptHandles) {
            if (response.hasFailed()) {
                doError(new BatchRequestFailedException("DeleteMessage", response.failed()));
            } else if (inFlightReceiptHandles.removeAll(receiptHandles)) {
                maybeScheduleMessageReception();
            }
        }

        private void maybeChangeMessageVisibility(String receiptHandle, Duration timeout) {
            createChangeMessageVisibilities(Collections.singletonList(receiptHandle), timeout, phase -> phase == 0)
                .subscribe(response -> handleMessageVisibilitiesChanged(response, Collections.emptyList()), this::doError);
        }

        private void maybeChangeMessageVisibilityAndMarkNotInFlight(String receiptHandle, Duration timeout) {
            List<String> receiptHandles = Collections.singletonList(receiptHandle);
            createChangeMessageVisibilities(receiptHandles, timeout, phase -> phase == 0)
                .subscribe(response -> handleMessageVisibilitiesChanged(response, receiptHandles), this::doError);
        }

        private Mono<ChangeMessageVisibilityBatchResponse> createChangeMessageVisibilities(
            Collection<String> receiptHandles,
            Duration timeout,
            IntPredicate phaseMustMatch
        ) {
            if (receiptHandles.isEmpty()) return Mono.empty();
            int timeoutInSeconds = Math.toIntExact(timeout.getSeconds());
            List<ChangeMessageVisibilityBatchRequestEntry> entries = receiptHandles.stream()
                .map(receiptHandle -> createChangeMessageVisibilityRequestEntry(receiptHandle, timeoutInSeconds))
                .collect(Collectors.toList());
            return maybeExecute(
                SqsAsyncClient::changeMessageVisibilityBatch,
                ChangeMessageVisibilityBatchRequest.builder().queueUrl(queueUrl).entries(entries).build(),
                phaseMustMatch
            );
        }

        private ChangeMessageVisibilityBatchRequestEntry createChangeMessageVisibilityRequestEntry(
            String receiptHandle,
            int timeoutInSeconds
        ) {
            return ChangeMessageVisibilityBatchRequestEntry.builder()
                .id(newReceiptHandleId())
                .receiptHandle(receiptHandle)
                .visibilityTimeout(timeoutInSeconds)
                .build();
        }

        private void handleMessageVisibilitiesChanged(
            ChangeMessageVisibilityBatchResponse response,
            Collection<String> receiptHandlesNoLongerInFlight
        ) {
            if (response.hasFailed()) {
                doError(new BatchRequestFailedException("ChangeMessageVisibility", response.failed()));
            } else if (inFlightReceiptHandles.removeAll(receiptHandlesNoLongerInFlight)) {
                maybeScheduleMessageReception();
            }
        }

        private <T, V> Mono<V> maybeExecute(
            BiFunction<SqsAsyncClient, T, CompletableFuture<V>> method,
            T request,
            IntPredicate phaseMustMatch
        ) {
            return Mono.fromSupplier(() -> phaseMustMatch.test(executionPhaser.register()))
                .cache()
                .flatMap(phaseMatched -> phaseMatched ? Mono.fromFuture(method.apply(client, request)) : Mono.empty())
                .retryWhen(DEFAULT_RETRY)
                .doFinally(__ -> executionPhaser.arriveAndDeregister());
        }

        private void doNext(SqsReceiverMessage sqsReceiverMessage) {
            try {
                subscriber.onNext(sqsReceiverMessage);
            } catch (Throwable error) {
                doError(error);
            }

            if (requestOutstanding.get() != Long.MAX_VALUE) {
                requestOutstanding.decrementAndGet();
            }
        }

        private void doError(Throwable error) {
            dispose().subscribe(wasDisposed -> {
                if (wasDisposed) {
                    subscriber.onError(error);
                }
            });
        }

        private <T> Flux<List<T>> batch(Flux<T> flux, int maxSize, Duration maxDuration) {
            return maxSize <= 1 ? flux.map(Collections::singletonList) : flux.bufferTimeout(maxSize, maxDuration);
        }

        private String newReceiptHandleId() {
            return UUID.randomUUID().toString();
        }
    }
}
