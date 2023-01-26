package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Configures behavior of receiving {@link SqsMessage}s and management of underlying SQS Client.
 */
public final class SqsReceiverOptions {

    public static final int DEFAULT_MAX_MESSAGES_PER_RECEPTION = 10;

    public static final int DEFAULT_WAIT_TIME_SECONDS_PER_RECEPTION = 0;

    public static final int DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;

    public static final int DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION = 4096;

    public static final int DEFAULT_DELETE_BATCH_SIZE = 10;

    public static final Duration DEFAULT_DELETE_INTERVAL = Duration.ofSeconds(1);

    public static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    private final Supplier<SqsAsyncClient> clientSupplier;

    private final int maxMessagesPerReception;

    private final Set<String> messageAttributesToRequest;

    private final Set<String> messageSystemAttributesToRequest;

    private final int waitTimeSecondsPerReception;

    private final int visibilityTimeoutSeconds;

    private final int maxInFlightPerSubscription;

    private final int deleteBatchSize;

    private final Duration deleteInterval;

    private final Duration closeTimeout;

    private SqsReceiverOptions(
        Supplier<SqsAsyncClient> clientSupplier,
        int maxMessagesPerReception,
        Set<String> messageAttributesToRequest,
        Set<String> messageSystemAttributesToRequest,
        int waitTimeSecondsPerReception,
        int visibilityTimeoutSeconds,
        int maxInFlightPerSubscription,
        int deleteBatchSize,
        Duration deleteInterval,
        Duration closeTimeout
    ) {
        this.clientSupplier = clientSupplier;
        this.maxMessagesPerReception = maxMessagesPerReception;
        this.messageAttributesToRequest = messageAttributesToRequest;
        this.messageSystemAttributesToRequest = messageSystemAttributesToRequest;
        this.waitTimeSecondsPerReception = waitTimeSecondsPerReception;
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
        this.maxInFlightPerSubscription = maxInFlightPerSubscription;
        this.deleteBatchSize = deleteBatchSize;
        this.deleteInterval = deleteInterval;
        this.closeTimeout = closeTimeout;
    }

    /**
     * Creates a new instance with the provided Client Supplier and default options. Note that a
     * new Client is created per subscription and closed when that subscription is terminated.
     *
     * @param clientSupplier The Supplier of an {@link SqsAsyncClient} invoked per subscription
     * @return A new {@link SqsReceiverOptions} instance
     */
    public static SqsReceiverOptions defaultOptions(Supplier<SqsAsyncClient> clientSupplier) {
        return newBuilder(clientSupplier).build();
    }

    /**
     * Creates a new (mutable) {@link Builder} with the provided Client Supplier and initialized
     * with default options.
     *
     * @param clientSupplier The Supplier of an {@link SqsAsyncClient} invoked per subscription
     * @return A new (mutable) {@link Builder} instance
     */
    public static Builder newBuilder(Supplier<SqsAsyncClient> clientSupplier) {
        return new Builder(clientSupplier);
    }

    /**
     * Builds a new {@link SqsAsyncClient}.
     */
    public SqsAsyncClient createClient() {
        return clientSupplier.get();
    }

    /**
     * The maximum number of Messages to request in each SQS Receive Message Request
     */
    public int maxMessagesPerReception() {
        return maxMessagesPerReception;
    }

    /**
     * The Message Attributes to request from SQS, which, when available, will be populated on
     * {@link ReceivedSqsMessage}s.
     */
    public Set<String> messageAttributesToRequest() {
        return messageAttributesToRequest;
    }

    /**
     * The Message System Attributes to request from SQS, which, when available, will be populated
     * on {@link ReceivedSqsMessage}s. The available set of Attributes are documented in
     * {@link software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName}.
     */
    public Set<String> messageSystemAttributesToRequest() {
        return messageSystemAttributesToRequest;
    }

    /**
     * The wait time in seconds for each SQS Receive Request. Any positive value activates "long
     * polling".
     */
    public int waitTimeSecondsPerReception() {
        return waitTimeSecondsPerReception;
    }

    /**
     * For each message received from SQS, this is the initial amount of seconds that the message
     * will be invisible to other Receive Requests. If any given Message is not deleted nor has its
     * visibility reset to a positive number of seconds before this amount of seconds elapses, then
     * the Message may be received again, and the Message's original receipt handle will be no
     * longer be valid.
     */
    public int visibilityTimeoutSeconds() {
        return visibilityTimeoutSeconds;
    }

    /**
     * The maximum number of Messages that haven't been deleted or marked as no longer in flight
     * per subscription.
     */
    public int maxInFlightPerSubscription() {
        return maxInFlightPerSubscription;
    }

    /**
     * When deleting messages from SQS, this configures the batching size. A batch size
     * {@literal <=} 1 effectively disables batching such that each Message is deleted in its own
     * Request. A batch size {@literal >} 1 enables batching, and requires that
     * {@link #deleteInterval()} also be set to a positive Duration.
     */
    public int deleteBatchSize() {
        return deleteBatchSize;
    }

    /**
     * When delete batching is enabled, this configures the maximum amount of time that will be
     * waited for a batch to be filled before executing the batch. Must be positive when batch
     * deleting is enabled.
     */
    public Duration deleteInterval() {
        return deleteInterval;
    }

    /**
     * When a subscription to SQS Messages is terminated, this is the amount of time that will be
     * waited for in-flight Requests to be completed and in-flight Messages to be made visible
     * again.
     */
    public Duration closeTimeout() {
        return closeTimeout;
    }

    /**
     * A mutable builder used to construct new instances of {@link SqsReceiverOptions}.
     */
    public static final class Builder {

        private final Supplier<SqsAsyncClient> clientSupplier;

        private int maxMessagesPerReception = DEFAULT_MAX_MESSAGES_PER_RECEPTION;

        private Set<String> messageAttributesToRequest = Collections.emptySet();

        private Set<String> messageSystemAttributesToRequest = Collections.emptySet();

        private int waitTimeSecondsPerReception = DEFAULT_WAIT_TIME_SECONDS_PER_RECEPTION;

        private int visibilityTimeoutSeconds = DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

        private int maxInFlightPerSubscription = DEFAULT_MAX_IN_FLIGHT_PER_SUBSCRIPTION;

        private int deleteBatchSize = DEFAULT_DELETE_BATCH_SIZE;

        private Duration deleteInterval = DEFAULT_DELETE_INTERVAL;

        private Duration closeTimeout = DEFAULT_CLOSE_TIMEOUT;

        private Builder(Supplier<SqsAsyncClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
        }

        /**
         * Build a new instance of {@link SqsReceiverOptions} from the currently set properties.
         */
        public SqsReceiverOptions build() {
            return new SqsReceiverOptions(
                clientSupplier,
                maxMessagesPerReception,
                messageAttributesToRequest,
                messageSystemAttributesToRequest,
                waitTimeSecondsPerReception,
                visibilityTimeoutSeconds,
                maxInFlightPerSubscription,
                deleteBatchSize,
                deleteInterval,
                closeTimeout
            );
        }

        /**
         * The maximum number of Messages to request in each SQS Receive Message Request
         */
        public Builder maxMessagesPerReception(int maxMessagesPerReception) {
            if (maxMessagesPerReception < 1 || maxMessagesPerReception > 10) {
                throw new IllegalArgumentException("maxMessagesPerReception must be 1-10, got " + maxMessagesPerReception);
            }
            this.maxMessagesPerReception = maxMessagesPerReception;
            return this;
        }

        /**
         * The Message Attributes to request from SQS, which, when available, will be populated on
         * {@link ReceivedSqsMessage}s.
         */
        public Builder messageAttributesToRequest(Set<String> messageAttributesToRequest) {
            this.messageAttributesToRequest = messageAttributesToRequest;
            return this;
        }

        /**
         * The Message System Attributes to request from SQS, which, when available, will be
         * populated on {@link ReceivedSqsMessage}s. The available set of Attributes are documented
         * in {@link software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName}.
         */
        public Builder messageSystemAttributesToRequest(Set<String> messageSystemAttributesToRequest) {
            this.messageSystemAttributesToRequest = messageSystemAttributesToRequest;
            return this;
        }

        /**
         * The wait time in seconds for each SQS Receive Request. Any positive value activates
         * "long polling".
         */
        public Builder waitTimeSecondsPerReception(int waitTimeSecondsPerReception) {
            this.waitTimeSecondsPerReception = waitTimeSecondsPerReception;
            return this;
        }

        /**
         * For each message received from SQS, this is the initial amount of seconds that the
         * message will be invisible to other Receive Requests. If any given Message is not deleted
         * nor has its visibility reset to a positive number of seconds before this amount of
         * seconds elapses, then the Message may be received again, and the Message's original
         * receipt handle will be no longer be valid.
         */
        public Builder visibilityTimeoutSeconds(int visibilityTimeoutSeconds) {
            this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
            return this;
        }

        /**
         * The maximum number of Messages that haven't been deleted or marked as no longer in flight
         * per subscription.
         */
        public Builder maxInFlightPerSubscription(int maxInFlightPerSubscription) {
            this.maxInFlightPerSubscription = maxInFlightPerSubscription;
            return this;
        }

        /**
         * When deleting messages from SQS, this configures the batching size. A batch size
         * {@literal <=} 1 effectively disables batching such that each Message is deleted in its
         * own Request. A batch size {@literal >} 1 enables batching, and requires that
         * {@link #deleteInterval()} also be set to a positive Duration.
         */
        public Builder deleteBatchSize(int deleteBatchSize) {
            this.deleteBatchSize = deleteBatchSize;
            return this;
        }

        /**
         * When delete batching is enabled, this configures the maximum amount of time that will be
         * waited for a batch to be filled before executing the batch. Must be positive when batch
         * deleting is enabled.
         */
        public Builder deleteInterval(Duration deleteInterval) {
            this.deleteInterval = deleteInterval;
            return this;
        }

        /**
         * When a subscription to SQS Messages is terminated, this is the amount of time that will
         * be waited for in-flight Requests to be completed and in-flight Messages to be made
         * visible again.
         */
        public Builder closeTimeout(Duration closeTimeout) {
            this.closeTimeout = closeTimeout;
            return this;
        }
    }
}
