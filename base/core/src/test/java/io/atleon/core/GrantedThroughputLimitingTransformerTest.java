package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;

import java.time.Duration;

class GrantedThroughputLimitingTransformerTest {

    @Test
    public void concatMap_givenAsynchronousGrant_expectsDelayAfterReservationResult() {
        StepVerifier.withVirtualTime(() -> Flux.just(1L, 2L)
                        .transform(GrantedThroughputLimitingTransformer.newBuilder(DelayApplicator.<Long>concatMap())
                                .grantor(__ -> Mono.delay(Duration.ofMillis(100))
                                        .thenReturn(Duration.ofMillis(200).toNanos()))
                                .build()))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(299))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(1L)
                .expectNoEvent(Duration.ofMillis(299))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(2L)
                .verifyComplete();
    }

    @Test
    public void concatMap_givenDownstreamCancellation_expectsPendingReservationCancelled() {
        PublisherProbe<Long> reservation = PublisherProbe.of(Mono.never());

        StepVerifier.create(Flux.just(1L)
                        .transform(GrantedThroughputLimitingTransformer.newBuilder(DelayApplicator.<Long>concatMap())
                                .grantor(__ -> reservation.mono())
                                .build()))
                .then(reservation::assertWasSubscribed)
                .thenCancel()
                .verify();

        reservation.assertWasCancelled();
    }
}
