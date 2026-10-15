package io.atleon.core;

import reactor.core.publisher.Mono;

/**
 * API through which throughput capacity is reserved for elements. Reservation of throughput
 * capacity may require waiting until the emitted number of nanoseconds has elapsed.
 *
 * <p>A grantor supplied to a throughput-limiting transformer is shared across subscriptions and
 * inner streams. Implementations must support concurrent reservation calls and concurrent
 * subscriptions to their returned publishers.
 */
interface ThroughputGrantor<T> {

    /**
     * Creates reservation of throughput capacity for the provided element. Returns a {@link Mono}
     * that may emit the number of nanoseconds to wait before safely using the reserved capacity.
     * This is an additional delay measured from receipt of the emitted value, after any time spent
     * obtaining the reservation, such as a remote service call. Emitting nothing or a non-positive
     * value is treated as "no wait necessary".
     *
     * @param t The element for which to reserve throughput capacity
     * @return A {@link Mono} emitting number of nanoseconds to wait before using reservation
     */
    Mono<Long> reserveCapacity(T t);
}
