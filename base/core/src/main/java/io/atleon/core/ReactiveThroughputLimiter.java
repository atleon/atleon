package io.atleon.core;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;

/**
 * Coordinates reactive throughput through state notifications and per-element capacity
 * reservations. Each downstream subscription mounts the limiter, obtaining a {@link Mount}
 * handle that delays elements until their reserved capacity is available. A limiter may be
 * shared by multiple concurrent mounts.
 *
 * @param <T> The type of elements upon which throughput limiting is enforced
 * @see ReactiveCircuitBreaker
 */
public interface ReactiveThroughputLimiter<T> {

    /**
     * Mounts the limiter and delivers the current state and subsequent notifications to
     * {@code currentStateConsumer} until unmounted. Reports limiter failures to
     * {@code errorConsumer} so the caller can terminate processing.
     */
    Mount<T> mount(Consumer<? super State> currentStateConsumer, Consumer<? super Throwable> errorConsumer);

    /**
     * Describes the current limiter state, including whether throughput is blocked and
     * processing must be suspended.
     */
    @FunctionalInterface
    interface State {

        /**
         * Returns whether processing must be suspended. An unblocked state allows capacity
         * reservations, which may still delay individual elements.
         */
        boolean blocked();
    }

    /**
     * Observes limiter state and reserves capacity for one downstream subscription, including
     * periods when its source is disconnected. The mount remains active for reconnection until
     * unmounted and may be shared by concurrently processing groups.
     *
     * @param <T> The type of elements for which capacity is reserved
     */
    interface Mount<T> {

        /**
         * Reserves capacity for the given element. The caller subscribes to the returned
         * publisher and waits for successful completion before allowing the element to proceed.
         * Completion may be immediate or delayed; any emitted value is ignored. The publisher
         * may signal an error if capacity cannot be reserved.
         *
         * <p>The caller is responsible for suspending processing while {@link State#blocked()}
         * is {@code true}; capacity reservations need not enforce the blocked state themselves.
         */
        Mono<?> reserveCapacity(T t);

        /**
         * Unmounts the limiter and releases its state-listener and capacity-management resources
         * when the downstream subscription terminates or is canceled. The default implementation
         * does nothing.
         */
        default void unmount() {}
    }
}
