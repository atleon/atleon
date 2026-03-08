package io.atleon.aws.sqs;

import reactor.core.publisher.Flux;

/**
 * A low-level receiver of {@link SqsMessage}s. Received messages contain the raw String body
 * payloads as received from requests to SQS. Each Subscription to Messages is backed by its own
 * {@link software.amazon.awssdk.services.sqs.SqsAsyncClient} which is disposed/closed upon
 * termination of the Subscription.
 */
public final class SqsReceiver {

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
        return receiveManual(new PollingSubscriptionFactory(options), queueUrl);
    }

    private Flux<SqsReceiverMessage> receiveManual(PollingSubscriptionFactory subscriptionFactory, String queueUrl) {
        return Flux.from(it -> it.onSubscribe(subscriptionFactory.create(queueUrl, it)));
    }
}
