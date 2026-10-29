package io.atleon.core;

import reactor.core.publisher.Mono;

/**
 * API through which throughput capacity is reserved for elements. Successful completion of a
 * reservation signals that the reserved capacity is ready to use.
 *
 * <p>A grantor supplied to a throughput-limiting transformer is shared across subscriptions and
 * inner streams. Implementations must support concurrent reservation calls and concurrent
 * subscriptions to their returned publishers.
 */
interface ThroughputGrantor<T> {

    /**
     * Creates a reservation of throughput capacity for the provided element. The returned
     * {@link Mono} must complete successfully only when the reserved capacity is ready to use,
     * including any time needed to obtain the reservation or wait for capacity to become available.
     * Any emitted value is ignored. Immediate successful completion, including empty completion,
     * means no wait is necessary. Errors prevent the element from being forwarded and propagate
     * to the transformed stream.
     *
     * @param t The element for which to reserve throughput capacity
     * @return A {@link Mono} whose successful completion signals that capacity is ready to use
     */
    Mono<?> reserveCapacity(T t);
}
