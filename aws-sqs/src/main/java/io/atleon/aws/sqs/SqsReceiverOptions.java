package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;

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

    public static final int DEFAULT_CLOSE_TIMEOUT_SECONDS = 10;

    private final Supplier<SqsAsyncClient> clientSupplier;

    private final int maxMessagesPerReception;

    private final Set<String> messageAttributesToRequest;

    private final Set<String> messageSystemAttributesToRequest;

    private final int waitTimeSecondsPerReception;

    private final int visibilityTimeoutSeconds;

    private final int maxInFlightPerSubscription;

    private final int closeTimeoutSeconds;

    private SqsReceiverOptions(
        Supplier<SqsAsyncClient> clientSupplier,
        int maxMessagesPerReception,
        Set<String> messageAttributesToRequest,
        Set<String> messageSystemAttributesToRequest,
        int waitTimeSecondsPerReception,
        int visibilityTimeoutSeconds,
        int maxInFlightPerSubscription,
        int closeTimeoutSeconds
    ) {
        this.clientSupplier = clientSupplier;
        this.maxMessagesPerReception = maxMessagesPerReception;
        this.messageAttributesToRequest = messageAttributesToRequest;
        this.messageSystemAttributesToRequest = messageSystemAttributesToRequest;
        this.waitTimeSecondsPerReception = waitTimeSecondsPerReception;
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
        this.maxInFlightPerSubscription = maxInFlightPerSubscription;
        this.closeTimeoutSeconds = closeTimeoutSeconds;
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
     * When a subscription to SQS Messages is terminated, this is the amount of seconds that will
     * be waited for in-flight Requests to be completed and in-flight Messages to be made visible
     * again.
     */
    public int closeTimeoutSeconds() {
        return closeTimeoutSeconds;
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

        private int closeTimeoutSeconds = DEFAULT_CLOSE_TIMEOUT_SECONDS;

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
                closeTimeoutSeconds
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
         * When a subscription to SQS Messages is terminated, this is the amount of seconds that
         * will be waited for in-flight Requests to be completed and in-flight Messages to be made
         * visible again.
         */
        public Builder closeTimeoutSeconds(int closeTimeoutSeconds) {
            this.closeTimeoutSeconds = closeTimeoutSeconds;
            return this;
        }
    }
}
