package io.atleon.core;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Coordinates reactive processing through state notifications and per-element permissions.
 * Each downstream subscription mounts the breaker, obtaining a {@link Mounted} handle that
 * provides a {@link Permitter} for each source connection. A breaker may be shared by multiple
 * concurrent mounts.
 */
interface ReactiveCircuitBreaker {

    /**
     * Mounts the breaker and delivers the current state and subsequent notifications to
     * the consumer until unmounted. Notify mounted consumers when denied permission requests may
     * be retried, even if {@link State#tripped()} is unchanged.
     */
    Mounted mount(Consumer<? super State> currentStateConsumer);

    /**
     * Describes the current breaker state, including whether it has tripped and processing must
     * be suspended.
     */
    interface State {

        /**
         * Returns whether the breaker is open and processing must be suspended. Closed and
         * half-open states return {@code false}, allowing individual permission requests.
         */
        boolean tripped();
    }

    /**
     * Observes breaker state for one downstream subscription, including periods when its source
     * is disconnected.
     */
    interface Mounted {

        /**
         * Returns a permitter for a source subscription, initially or on reconnection. This
         * operation must not end the mounted breaker's state notifications.
         */
        Permitter connect();

        /**
         * Unmounts the breaker and releases its state-listener resources. Each connected
         * permitter is disconnected separately by the caller. The default implementation does
         * nothing.
         */
        default void unmount() {}
    }

    /**
     * Acquires processing permissions for one source connection and handles its cleanup.
     */
    interface Permitter {

        /**
         * Immediately reserves permission to process one element, or returns empty if denied.
         * Implementations must return empty while the breaker is tripped. Permission may be denied
         * while not tripped, such as when half-open probes are in flight.
         */
        Optional<Permit> tryAcquirePermit();

        /**
         * Cleans up this connection, including any unsettled reservations, when the source
         * terminates or is canceled. The mounted breaker remains active for reconnection until
         * unmounted. The default implementation does nothing.
         */
        default void disconnect() {}
    }

    /**
     * Receives the processing outcome of a reserved permission. Callbacks must not throw and may
     * run on different threads, including after the associated permitter is disconnected.
     * Implementations should guarantee joint threadsafe idempotency of settlement: at most one
     * outcome should be recorded, and subsequent settlement attempts must be ignored. The caller
     * does not enforce this guarantee.
     */
    interface Permit {

        /**
         * Reports processing failure with its cause.
         */
        void error(Throwable error);

        /**
         * Reports successful processing.
         */
        void success();
    }
}
