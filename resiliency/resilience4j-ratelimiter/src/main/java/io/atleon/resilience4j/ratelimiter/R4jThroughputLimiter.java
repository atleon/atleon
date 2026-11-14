package io.atleon.resilience4j.ratelimiter;

import io.atleon.core.ReactiveThroughputLimiter;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * Adapts a Resilience4j {@link RateLimiter} to reactive capacity reservations shared across mounts.
 * Permits are reserved on subscription, delaying without blocking or failing with
 * {@link RequestNotPermitted} if the configured timeout is insufficient.
 *
 * <p>Requires support for {@link RateLimiter#reservePermission(int)}. Cancellation does not
 * refund permits; the supplied rate limiter and scheduler remain owned by the caller.
 *
 * @param <T> the type of elements being limited
 */
public final class R4jThroughputLimiter<T> implements ReactiveThroughputLimiter<T> {

    private final RateLimiter rateLimiter;

    private final ToIntFunction<? super T> weighting;

    private final Scheduler scheduler;

    private R4jThroughputLimiter(RateLimiter rateLimiter, ToIntFunction<? super T> weighting, Scheduler scheduler) {
        this.rateLimiter = rateLimiter;
        this.weighting = weighting;
        this.scheduler = scheduler;
    }

    /**
     * Creates a limiter reserving one permit per element, with delays on
     * {@link Schedulers#parallel()}.
     */
    public static <T> R4jThroughputLimiter<T> create(RateLimiter rateLimiter) {
        return create(rateLimiter, __ -> 1, Schedulers.parallel());
    }

    /**
     * Creates a limiter reserving one permit per element, with delays on the given scheduler.
     */
    public static <T> R4jThroughputLimiter<T> create(RateLimiter rateLimiter, Scheduler scheduler) {
        return create(rateLimiter, __ -> 1, scheduler);
    }

    /**
     * Creates a limiter with per-element permit weights and the given delay scheduler.
     * Weighting is evaluated synchronously when capacity is requested. Zero weights bypass
     * reservation; negative weights produce a reactive {@link IllegalArgumentException}.
     */
    public static <T> R4jThroughputLimiter<T> create(
            RateLimiter rateLimiter, ToIntFunction<? super T> weighting, Scheduler scheduler) {
        return new R4jThroughputLimiter<>(rateLimiter, weighting, scheduler);
    }

    /**
     * Immediately reports an unblocked state and returns a handle sharing the rate limiter.
     * Reservation errors are emitted by the returned publishers; unmounting is a no-op.
     */
    @Override
    public Mount<T> mount(Consumer<? super State> currentStateConsumer, Consumer<? super Throwable> errorConsumer) {
        currentStateConsumer.accept((State) () -> false);
        return new R4jMount();
    }

    private final class R4jMount implements Mount<T> {

        @Override
        public Mono<?> reserveCapacity(T t) {
            int weight = weighting.applyAsInt(t);
            return Mono.defer(() -> delay(weight));
        }

        private Mono<?> delay(int weight) {
            long delayNanos = weight > 0 ? rateLimiter.reservePermission(weight) : weight;
            if (delayNanos == 0) {
                return Mono.empty();
            } else if (delayNanos > 0) {
                return Mono.delay(Duration.ofNanos(delayNanos), scheduler);
            } else if (weight < 0) {
                return Mono.error(new IllegalArgumentException("Weight must be non-negative: " + weight));
            } else {
                return Mono.error(RequestNotPermitted.createRequestNotPermitted(rateLimiter));
            }
        }
    }
}
