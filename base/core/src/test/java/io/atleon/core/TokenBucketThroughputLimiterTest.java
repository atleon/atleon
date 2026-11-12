package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TokenBucketThroughputLimiterTest {

    @Test
    void givenEmptyLimits_expectsUnrestrictedReservations() {
        StepVerifier.withVirtualTime(
                        () -> reserve(TokenBucketThroughputLimiter.<Long>newBuilder(), Flux.just(1L, 2L, 3L)))
                .expectNext(1L, 2L, 3L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenInfiniteLimit_expectsNoDelay() {
        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limit(Rate.infinite()), Flux.just(1L, 2L, 3L)))
                .expectNext(1L, 2L, 3L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenPositiveLimit_expectsDelayedElements() {
        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limit(Rate.perSecond(1)),
                        Flux.just(1L, 2L, 3L)))
                .expectNext(1L)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2L)
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(3L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenLimitIndivisibleByDuration_expectsExactReplenishment() {
        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limit(Rate.perSecond(3)),
                        Flux.just(1L, 2L, 3L, 4L, 5L, 6L)))
                .expectNext(1L, 2L, 3L)
                .expectNoEvent(Duration.ofMillis(333))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(4L)
                .expectNoEvent(Duration.ofMillis(332))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(5L)
                .expectNoEvent(Duration.ofMillis(332))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(6L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenWeighting_expectsProportionalDelay() {
        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder()
                                .limit(Rate.perSecond(2))
                                .weighting(__ -> 2),
                        Flux.just(1L, 2L)))
                .expectNext(1L)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenZeroWeighting_expectsNoDelay() {
        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder()
                                .limit(Rate.perSecond(1))
                                .weighting(__ -> 0),
                        Flux.just(1L, 2L, 3L)))
                .expectNext(1L, 2L, 3L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenNegativeWeighting_expectsError() {
        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder()
                                .limit(Rate.perSecond(1))
                                .weighting(__ -> -1),
                        Flux.just(1L)))
                .expectError(IllegalArgumentException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenZeroRatePause_expectsCanceledReservationReplenished() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        Sinks.Many<Rate> limits = Sinks.many().replay().latest();
        limits.tryEmitNext(Rate.perSecond(1));
        List<Long> received = new ArrayList<>();
        TokenBucketThroughputLimiter<Long> limiter = TokenBucketThroughputLimiter.<Long>newBuilder()
                .limits(limits.asFlux())
                .scheduler(clock)
                .build();
        Disposable subscription = Flux.just(1L, 2L)
                .<Alo<Long>>map(value -> new ComposedAlo<>(value, () -> {}, __ -> {}))
                .transform(ThroughputLimitingTransformer.alo(limiter, 0))
                .map(Alo::get)
                .subscribe(received::add);

        try {
            assertEquals(List.of(1L), received);
            // Cancel the second element's reservation, then resubscribe to the source.
            clock.advanceTimeBy(Duration.ofMillis(100));
            limits.tryEmitNext(Rate.zero());
            clock.advanceTimeBy(Duration.ofMillis(100));
            limits.tryEmitNext(Rate.perSecond(1));

            // The consumed token remains charged, but the canceled reservation adds no delay.
            clock.advanceTimeBy(Duration.ofMillis(799));
            assertEquals(List.of(1L), received);
            clock.advanceTimeBy(Duration.ofMillis(1));
            assertEquals(List.of(1L, 1L), received);
            clock.advanceTimeBy(Duration.ofSeconds(1));
            assertEquals(List.of(1L, 1L, 2L), received);
        } finally {
            subscription.dispose();
            clock.dispose();
        }
    }

    @Test
    void givenReservationQueuedBeforeZeroRatePause_expectsCanceledReservationReplenished() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        Sinks.Many<Rate> limits = Sinks.many().replay().latest();
        limits.tryEmitNext(Rate.perSecond(1));
        Disposable.Swap reservation = Disposables.swap();
        ReactiveThroughputLimiter.Mount<Long> mount = TokenBucketThroughputLimiter.<Long>newBuilder()
                .limits(limits.asFlux())
                .scheduler(clock)
                .build()
                .mount(
                        state -> {
                            if (state.blocked()) {
                                reservation.dispose();
                            }
                        },
                        __ -> {});

        try {
            mount.reserveCapacity(1L)
                    .doOnSuccess(__ -> {
                        // The first reservation's callback still occupies the drain, so the
                        // second reservation queues before the zero-rate notification.
                        reservation.update(mount.reserveCapacity(2L).subscribe());
                        limits.tryEmitNext(Rate.zero());
                    })
                    .block(Duration.ofSeconds(5));
            assertTrue(reservation.get().isDisposed());
            limits.tryEmitNext(Rate.perSecond(1));

            // Only the consumed token remains charged: the next grant is at 1s, not 2s.
            StepVerifier.withVirtualTime(() -> mount.reserveCapacity(3L).thenReturn(3L), () -> clock, Long.MAX_VALUE)
                    .expectSubscription()
                    .expectNoEvent(Duration.ofMillis(999))
                    .thenAwait(Duration.ofMillis(1))
                    .expectNext(3L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));
        } finally {
            reservation.dispose();
            mount.unmount();
            clock.dispose();
        }
    }

    @Test
    void givenCanceledReservationWithoutDelay_expectsTokensReplenished() {
        VirtualTimeScheduler clock = VirtualTimeScheduler.create();
        Sinks.Many<Rate> limits = Sinks.many().replay().latest();
        Sinks.Many<Alo<Long>> first = Sinks.many().multicast().onBackpressureBuffer(256, false);
        Sinks.Many<Alo<Long>> second = Sinks.many().multicast().onBackpressureBuffer(256, false);
        List<Long> received = new ArrayList<>();
        limits.tryEmitNext(Rate.perSecond(2));

        TokenBucketThroughputLimiter<Long> limiter = TokenBucketThroughputLimiter.<Long>newBuilder()
                .limits(limits.asFlux())
                .scheduler(clock)
                .build();
        Disposable subscription = Flux.just(
                        new AloGroupedFlux<>(first.asFlux(), "first"), new AloGroupedFlux<>(second.asFlux(), "second"))
                .transform(ThroughputLimitingTransformer.aloGroups(limiter, 0))
                .flatMap(AloFlux::toFlux)
                .map(Alo::get)
                .doOnNext(value -> {
                    if (value == 1L) {
                        // The drain is still delivering the first reservation. Queue the pause
                        // before a reservation for the remaining immediately available token.
                        limits.tryEmitNext(Rate.zero());
                        second.tryEmitNext(alo(2L));
                    }
                })
                .subscribe(received::add);

        try {
            first.tryEmitNext(alo(1L));
            assertEquals(List.of(1L), received);

            limits.tryEmitNext(Rate.perSecond(2));
            first.tryEmitNext(alo(3L));
            // The canceled reservation is refunded even though it never needed a timer.
            assertEquals(List.of(1L, 3L), received);

            // Both successfully delivered elements remain charged.
            first.tryEmitNext(alo(4L));
            clock.advanceTimeBy(Duration.ofMillis(499));
            assertEquals(List.of(1L, 3L), received);
            clock.advanceTimeBy(Duration.ofMillis(1));
            assertEquals(List.of(1L, 3L, 4L), received);
        } finally {
            subscription.dispose();
            clock.dispose();
        }
    }

    @Test
    void givenLimitUpdates_expectsBlockedFiniteAndInfiniteStates() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        List<Boolean> blockedStates = new ArrayList<>();
        ReactiveThroughputLimiter.Mount<Long> mount = TokenBucketThroughputLimiter.<Long>newBuilder()
                .limits(limits.asFlux())
                .build()
                .mount(state -> blockedStates.add(state.blocked()), __ -> {});

        limits.tryEmitNext(Rate.perSecond(1));
        limits.tryEmitNext(Rate.zero());
        limits.tryEmitNext(Rate.infinite());

        assertEquals(Arrays.asList(false, true, false), blockedStates);
        mount.unmount();
    }

    @Test
    void givenLimitPublisherError_expectsErrorCallback() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        AtomicReference<Throwable> receivedError = new AtomicReference<>();
        ReactiveThroughputLimiter.Mount<Long> mount = TokenBucketThroughputLimiter.<Long>newBuilder()
                .limits(limits.asFlux())
                .build()
                .mount(__ -> {}, receivedError::set);
        RuntimeException failure = new RuntimeException("limit failure");

        limits.tryEmitError(failure);

        assertSame(failure, receivedError.get());
        mount.unmount();
    }

    @Test
    void givenLimitChangesFromFiniteToInfinite_expectsDelayRemoved() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> values = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limits(limits.asFlux()), values.asFlux()))
                .then(() -> limits.tryEmitNext(Rate.perSecond(1)))
                .then(() -> values.tryEmitNext(1L))
                .expectNext(1L)
                .then(() -> limits.tryEmitNext(Rate.infinite()))
                .then(() -> values.tryEmitNext(2L))
                .expectNext(2L)
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenLimitChangesFromInfiniteToFinite_expectsDelayApplied() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> values = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limits(limits.asFlux()), values.asFlux()))
                .then(() -> limits.tryEmitNext(Rate.infinite()))
                .then(() -> values.tryEmitNext(1L))
                .expectNext(1L)
                .then(() -> limits.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    values.tryEmitNext(2L);
                    values.tryEmitNext(3L);
                })
                .expectNext(2L)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(3L)
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenReservationBacklog_whenLimitChangesThroughInfinite_expectsFullBucketAndExistingDelaysPreserved() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> values = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> reserveConcurrently(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limits(limits.asFlux()), values.asFlux()))
                .then(() -> limits.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    values.tryEmitNext(1L);
                    values.tryEmitNext(2L);
                    values.tryEmitNext(3L);
                })
                .expectNext(1L)
                .then(() -> limits.tryEmitNext(Rate.infinite()))
                .then(() -> values.tryEmitNext(4L))
                .expectNext(4L)
                .then(() -> limits.tryEmitNext(Rate.perSecond(2)))
                .then(() -> {
                    values.tryEmitNext(5L);
                    values.tryEmitNext(6L);
                    values.tryEmitNext(7L);
                    values.tryEmitComplete();
                })
                .expectNext(5L, 6L)
                .expectNoEvent(Duration.ofMillis(499))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(7L)
                .expectNoEvent(Duration.ofMillis(499))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2L)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(3L)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenLimitIncrease_expectsFasterReservations() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> values = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> reserve(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limits(limits.asFlux()), values.asFlux()))
                .then(() -> limits.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    values.tryEmitNext(1L);
                    values.tryEmitNext(2L);
                })
                .expectNext(1L)
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(2L)
                .then(() -> limits.tryEmitNext(Rate.perSecond(100)))
                .then(() -> values.tryEmitNext(3L))
                .thenAwait(Duration.ofMillis(10))
                .expectNext(3L)
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenReservationBacklog_expectsHigherLimitAppliedToFutureReservation() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> values = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> reserveConcurrently(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limits(limits.asFlux()), values.asFlux()))
                .then(() -> limits.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    for (long value = 0; value < 12; value++) {
                        values.tryEmitNext(value);
                    }
                })
                .expectNext(0L)
                .then(() -> limits.tryEmitNext(Rate.perSecond(2)))
                .then(() -> values.tryEmitNext(100L))
                .thenAwait(Duration.ofSeconds(11))
                .expectNext(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L)
                .thenAwait(Duration.ofMillis(500))
                .expectNext(100L)
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void givenReservationBacklog_expectsLowerLimitAppliedToFutureReservation() {
        Sinks.Many<Rate> limits = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> values = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> reserveConcurrently(
                        TokenBucketThroughputLimiter.<Long>newBuilder().limits(limits.asFlux()), values.asFlux()))
                .then(() -> limits.tryEmitNext(Rate.perSecond(2)))
                .then(() -> {
                    for (long value = 0; value < 10; value++) {
                        values.tryEmitNext(value);
                    }
                })
                .expectNext(0L, 1L)
                .then(() -> limits.tryEmitNext(Rate.perSecond(1)))
                .then(() -> values.tryEmitNext(100L))
                .thenAwait(Duration.ofSeconds(4))
                .expectNext(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
                .expectNoEvent(Duration.ofMillis(999))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(100L)
                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    private static Alo<Long> alo(long value) {
        return new ComposedAlo<>(value, () -> {}, __ -> {});
    }

    private static Flux<Long> reserve(TokenBucketThroughputLimiter.Builder<Long> builder, Flux<Long> values) {
        return Flux.defer(() -> {
            ReactiveThroughputLimiter.Mount<Long> mount = builder.build().mount(__ -> {}, __ -> {});
            return values.concatMap(value -> mount.reserveCapacity(value).thenReturn(value), 0)
                    .doFinally(__ -> mount.unmount());
        });
    }

    private static Flux<Long> reserveConcurrently(
            TokenBucketThroughputLimiter.Builder<Long> builder, Flux<Long> values) {
        return Flux.defer(() -> {
            ReactiveThroughputLimiter.Mount<Long> mount = builder.build().mount(__ -> {}, __ -> {});
            return values.flatMap(value -> mount.reserveCapacity(value).thenReturn(value))
                    .doFinally(__ -> mount.unmount());
        });
    }
}
