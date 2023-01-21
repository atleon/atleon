package io.atleon.aws.sqs;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Configures behavior of sending {@link SqsMessage}s and creation of underlying SQS Client.
 */
public final class SqsSenderOptions {

    public static final int DEFAULT_BATCH_SIZE = 1;

    public static final Duration DEFAULT_BATCH_DURATION = Duration.ZERO;

    public static final int DEFAULT_MAX_REQUESTS_IN_FLIGHT = 1;

    private final Supplier<SqsAsyncClient> clientSupplier;

    private final int batchSize;

    private final Duration batchDuration;

    private final int maxRequestsInFlight;

    private SqsSenderOptions(
        Supplier<SqsAsyncClient> clientSupplier,
        int batchSize,
        Duration batchDuration,
        int maxRequestsInFlight
    ) {
        this.clientSupplier = clientSupplier;
        this.batchSize = batchSize;
        this.batchDuration = batchDuration;
        this.maxRequestsInFlight = maxRequestsInFlight;
    }

    /**
     * Creates a new instance with the provided Client Supplier and default options. Each Sender
     * will reference at most one non-closed Client at any given time.
     *
     * @param clientSupplier The Supplier of an {@link SqsAsyncClient} invoked per subscription
     * @return A new {@link SqsSenderOptions} instance
     */
    public static SqsSenderOptions defaultOptions(Supplier<SqsAsyncClient> clientSupplier) {
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
     * When sending multiple messages to SQS, this configures the batching size. A batch size <= 1
     * effectively disables batching such that each Message is sent in its own Request. A batch
     * size > 1 enables batching, and requires that {@link #batchDuration()} also be set to a
     * positive Duration.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * When batching is enabled, this configures the maximum amount of time that will be waited for
     * a batch to be filled before sending the batch. Must be positive when batching is enabled.
     */
    public Duration batchDuration() {
        return batchDuration;
    }

    /**
     * The maximum amount of concurrent SQS Send Requests that are allowed to be in flight per sent
     * Publisher. If batching is disabled, this is the maximum number of Messages in flight. If
     * batching is enabled, this is the maximum number of batched requests in flight.
     */
    public int maxRequestsInFlight() {
        return maxRequestsInFlight;
    }

    /**
     * A mutable builder used to construct new instances of {@link SqsSenderOptions}.
     */
    public static final class Builder {

        private final Supplier<SqsAsyncClient> clientSupplier;

        private int batchSize = DEFAULT_BATCH_SIZE;

        private Duration batchDuration = DEFAULT_BATCH_DURATION;

        private int maxRequestsInFlight = DEFAULT_MAX_REQUESTS_IN_FLIGHT;

        private Builder(Supplier<SqsAsyncClient> clientSupplier) {
            this.clientSupplier = clientSupplier;
        }

        /**
         * Build a new instance of {@link SqsSenderOptions} from the currently set properties.
         */
        public SqsSenderOptions build() {
            return new SqsSenderOptions(clientSupplier, batchSize, batchDuration, maxRequestsInFlight);
        }

        /**
         * When sending multiple messages to SQS, this configures the batching size. A batch size
         * <= 1 effectively disables batching such that each Message is sent in its own Request. A
         * batch size > 1 enables batching, and requires that {@link #batchDuration(Duration)} also
         * be set to a positive Duration.
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * When batching is enabled, this configures the maximum amount of time that will be waited
         * for a batch to be filled before sending the batch. Must be positive when batching is
         * enabled.
         */
        public Builder batchDuration(Duration batchDuration) {
            this.batchDuration = batchDuration;
            return this;
        }

        /**
         * The maximum amount of concurrent SQS Send Requests that are allowed to be in flight per
         * sent Publisher. If batching is disabled, this is the maximum number of Messages in
         * flight. If batching is enabled, this is the maximum number of batched requests in
         * flight.
         */
        public Builder maxRequestsInFlight(int maxRequestsInFlight) {
            this.maxRequestsInFlight = maxRequestsInFlight;
            return this;
        }
    }
}
