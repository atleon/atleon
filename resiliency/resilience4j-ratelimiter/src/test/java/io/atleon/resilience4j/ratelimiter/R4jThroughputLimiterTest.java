package io.atleon.resilience4j.ratelimiter;

import io.atleon.core.ReactiveThroughputLimiter;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import io.github.resilience4j.ratelimiter.internal.SemaphoreBasedRateLimiter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R4jThroughputLimiterTest {

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(5);

    private final VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    @Test
    void mount_givenExhaustedCapacity_expectsImmediateUnblockedState() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ZERO);
        rateLimiter.reservePermission();
        List<Boolean> states = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();

        R4jThroughputLimiter.create(rateLimiter).mount(state -> states.add(state.blocked()), errors::add);

        assertEquals(List.of(false), states);
        assertEquals(List.of(), errors);
    }

    @Test
    void reserveCapacity_givenAvailablePermits_expectsImmediateCompletion() {
        RateLimiter rateLimiter = newRateLimiter(2, Duration.ZERO);
        ReactiveThroughputLimiter.Mount<Integer> mount = mount(R4jThroughputLimiter.create(rateLimiter));

        StepVerifier.create(reserve(mount, 10, 20))
                .expectNext(10, 20)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        assertEquals(0, rateLimiter.getMetrics().getAvailablePermissions());
    }

    @Test
    void reserveCapacity_givenRepeatedSubscriptions_expectsNewReservationWithoutReweighting() {
        RateLimiter rateLimiter = newRateLimiter(2, Duration.ZERO);
        AtomicInteger weightedElements = new AtomicInteger();
        ReactiveThroughputLimiter.Mount<Integer> mount = mount(R4jThroughputLimiter.create(
                rateLimiter,
                __ -> {
                    weightedElements.incrementAndGet();
                    return 1;
                },
                scheduler));

        Mono<?> reservation = mount.reserveCapacity(1);

        assertEquals(1, weightedElements.get());
        assertEquals(2, rateLimiter.getMetrics().getAvailablePermissions());
        StepVerifier.create(reservation).expectComplete().verify(VERIFY_TIMEOUT);
        assertEquals(1, rateLimiter.getMetrics().getAvailablePermissions());
        StepVerifier.create(reservation).expectComplete().verify(VERIFY_TIMEOUT);
        StepVerifier.create(reservation).expectError(RequestNotPermitted.class).verify(VERIFY_TIMEOUT);
        assertEquals(1, weightedElements.get());
        assertEquals(0, rateLimiter.getMetrics().getAvailablePermissions());
    }

    @Test
    void reserveCapacity_givenExhaustedCapacity_expectsDelayUntilNextRefresh() {
        RateLimiter rateLimiter = newRateLimiter(2, Duration.ofSeconds(1));

        StepVerifier.withVirtualTime(
                        () -> reserve(mount(R4jThroughputLimiter.create(rateLimiter)), 1, 2, 3, 4, 5),
                        () -> scheduler,
                        Long.MAX_VALUE)
                .expectNext(1, 2)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(3, 4)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(5)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void reserveCapacity_givenWeightedElements_expectsProportionalReservationsAcrossPeriods() {
        RateLimiter rateLimiter = newRateLimiter(2, Duration.ofSeconds(2));
        ReactiveThroughputLimiter.Mount<Integer> mount =
                mount(R4jThroughputLimiter.create(rateLimiter, Integer::intValue, scheduler));

        StepVerifier.withVirtualTime(() -> reserve(mount, 2, 3, 1), () -> scheduler, Long.MAX_VALUE)
                .expectNext(2)
                .expectNoEvent(Duration.ofMillis(1999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(3, 1)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void reserveCapacity_givenZeroWeightAndExhaustedCapacity_expectsImmediateCompletion() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ZERO);
        rateLimiter.reservePermission();
        ReactiveThroughputLimiter.Mount<Integer> mount =
                mount(R4jThroughputLimiter.create(rateLimiter, __ -> 0, scheduler));

        StepVerifier.create(reserve(mount, 1, 2, 3))
                .expectNext(1, 2, 3)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        assertEquals(0, rateLimiter.getMetrics().getAvailablePermissions());
    }

    @Test
    void reserveCapacity_givenNegativeWeight_expectsErrorWithoutChangingCapacity() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ZERO);
        ReactiveThroughputLimiter.Mount<Integer> mount =
                mount(R4jThroughputLimiter.create(rateLimiter, __ -> -1, scheduler));

        StepVerifier.create(mount.reserveCapacity(1))
                .expectError(IllegalArgumentException.class)
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, rateLimiter.getMetrics().getAvailablePermissions());
    }

    @Test
    void reserveCapacity_givenWeightingFailure_expectsThrownErrorWithoutChangingCapacity() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ZERO);
        RuntimeException failure = new RuntimeException("weighting failed");
        ReactiveThroughputLimiter.Mount<Integer> mount = mount(R4jThroughputLimiter.create(
                rateLimiter,
                __ -> {
                    throw failure;
                },
                scheduler));

        assertSame(failure, assertThrows(RuntimeException.class, () -> mount.reserveCapacity(1)));

        assertEquals(1, rateLimiter.getMetrics().getAvailablePermissions());
    }

    @Test
    void reserveCapacity_givenInsufficientTimeout_expectsRejectionWithoutReserving() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ofMillis(999));
        ReactiveThroughputLimiter.Mount<Integer> mount = mount(R4jThroughputLimiter.create(rateLimiter));

        StepVerifier.create(reserve(mount, 1, 2))
                .expectNext(1)
                .expectErrorSatisfies(error ->
                        assertEquals(rateLimiter.getName(), ((RequestNotPermitted) error).getCausingRateLimiterName()))
                .verify(VERIFY_TIMEOUT);

        assertEquals(0, rateLimiter.getMetrics().getAvailablePermissions());
    }

    @Test
    void reserveCapacity_givenUnsupportedReservations_expectsReactiveError() {
        SemaphoreBasedRateLimiter rateLimiter =
                new SemaphoreBasedRateLimiter("semaphore", RateLimiterConfig.ofDefaults());
        try {
            ReactiveThroughputLimiter.Mount<Integer> mount = mount(R4jThroughputLimiter.create(rateLimiter));

            StepVerifier.create(mount.reserveCapacity(1))
                    .expectError(UnsupportedOperationException.class)
                    .verify(VERIFY_TIMEOUT);
        } finally {
            rateLimiter.shutdown();
        }
    }

    @Test
    void reserveCapacity_givenMultipleMountsAndExternalUser_expectsSharedCapacity() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ofSeconds(2));
        R4jThroughputLimiter<Integer> limiter = R4jThroughputLimiter.create(rateLimiter, scheduler);
        ReactiveThroughputLimiter.Mount<Integer> first = mount(limiter);
        ReactiveThroughputLimiter.Mount<Integer> second = mount(limiter);
        assertEquals(0, rateLimiter.reservePermission());

        StepVerifier.withVirtualTime(
                        () -> Flux.merge(reserve(first, 1), reserve(second, 2)), () -> scheduler, Long.MAX_VALUE)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(1)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void reserveCapacity_givenConcurrentReservations_expectsSharedLimit() throws Exception {
        RateLimiter rateLimiter = newRateLimiter(4, Duration.ZERO);
        R4jThroughputLimiter<Integer> limiter = R4jThroughputLimiter.create(rateLimiter);
        CyclicBarrier barrier = new CyclicBarrier(8);
        var executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<Boolean>> reservations = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                ReactiveThroughputLimiter.Mount<Integer> mount = mount(limiter);
                reservations.add(executor.submit(() -> {
                    barrier.await(5, TimeUnit.SECONDS);
                    return mount.reserveCapacity(1)
                            .thenReturn(true)
                            .onErrorReturn(RequestNotPermitted.class, false)
                            .block(VERIFY_TIMEOUT);
                }));
            }

            int permitted = 0;
            for (Future<Boolean> reservation : reservations) {
                if (reservation.get(5, TimeUnit.SECONDS)) {
                    permitted++;
                }
            }
            assertEquals(4, permitted);
            assertEquals(0, rateLimiter.getMetrics().getAvailablePermissions());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void reserveCapacity_givenConfigurationChanges_expectsUpdatedLimitAndTimeout() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ZERO);
        ReactiveThroughputLimiter.Mount<Integer> mount = mount(R4jThroughputLimiter.create(rateLimiter, scheduler));
        StepVerifier.create(mount.reserveCapacity(1)).expectComplete().verify(VERIFY_TIMEOUT);
        rateLimiter.changeTimeoutDuration(Duration.ofSeconds(1));
        rateLimiter.changeLimitForPeriod(2);

        StepVerifier.withVirtualTime(() -> reserve(mount, 2, 3), () -> scheduler, Long.MAX_VALUE)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2, 3)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void unmount_givenCanceledDelay_expectsOtherMountAndSchedulerToRemainUsable() {
        RateLimiter rateLimiter = newRateLimiter(1, Duration.ofSeconds(2));
        R4jThroughputLimiter<Integer> limiter = R4jThroughputLimiter.create(rateLimiter, scheduler);
        ReactiveThroughputLimiter.Mount<Integer> first = mount(limiter);
        ReactiveThroughputLimiter.Mount<Integer> second = mount(limiter);
        assertEquals(0, rateLimiter.reservePermission());
        AtomicInteger completions = new AtomicInteger();

        var reservation = first.reserveCapacity(1)
                .doOnSuccess(__ -> completions.incrementAndGet())
                .subscribe();
        reservation.dispose();
        first.unmount();
        first.unmount();

        assertFalse(scheduler.isDisposed());
        assertEquals(-1, rateLimiter.getMetrics().getAvailablePermissions());
        StepVerifier.withVirtualTime(() -> reserve(second, 2), () -> scheduler, Long.MAX_VALUE)
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(1999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2)
                .expectComplete()
                .verify(VERIFY_TIMEOUT);
        assertEquals(0, completions.get());
    }

    private RateLimiter newRateLimiter(int permits, Duration timeout) {
        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitForPeriod(permits)
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .timeoutDuration(timeout)
                .build();
        return RateLimiter.of("test", config, () -> scheduler.now(TimeUnit.NANOSECONDS));
    }

    private static <T> ReactiveThroughputLimiter.Mount<T> mount(R4jThroughputLimiter<T> limiter) {
        return limiter.mount(__ -> {}, error -> {
            throw new AssertionError("Unexpected limiter failure", error);
        });
    }

    private static Flux<Integer> reserve(ReactiveThroughputLimiter.Mount<Integer> mount, Integer... elements) {
        return Flux.just(elements)
                .concatMap(element -> mount.reserveCapacity(element).thenReturn(element));
    }
}
