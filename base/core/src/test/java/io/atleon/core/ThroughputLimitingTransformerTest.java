package io.atleon.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.function.Function;

class ThroughputLimitingTransformerTest {

    // ===== concatMap: No limit / unlimited scenarios =====

    @Test
    public void concatMap_givenEmptyLimits_expectsNoThroughputLimit() {
        StepVerifier.withVirtualTime(
                        () -> Flux.just(1L, 2L, 3L).transform(newConcatMap().build()))
                .expectNext(1L, 2L, 3L)
                .verifyComplete();
    }

    @Test
    public void concatMap_givenInfiniteLimit_expectsNoThroughputLimit() {
        StepVerifier.withVirtualTime(() -> Flux.just(1L, 2L, 3L)
                        .transform(newConcatMap()
                                .limits(Mono.just(Rate.infinite()))
                                .build()))
                .expectNext(1L, 2L, 3L)
                .verifyComplete();
    }

    // ===== concatMap: Zero limit scenarios =====

    @Test
    public void concatMap_givenZeroLimit_expectsNoElements() {
        StepVerifier.withVirtualTime(() -> Flux.just(1L, 2L, 3L)
                        .transform(newConcatMap().limits(Mono.just(Rate.zero())).build()))
                .expectSubscription()
                .expectNoEvent(Duration.ofSeconds(10))
                .thenCancel()
                .verify();
    }

    // ===== concatMap: Positive limit scenarios =====

    @Test
    public void concatMap_givenPositiveLimit_expectsDelayedElements() {
        StepVerifier.withVirtualTime(() -> Flux.just(1L, 2L, 3L)
                        .transform(newConcatMap()
                                .limits(Mono.just(Rate.perSecond(1)))
                                .build()))
                .expectNext(1L)
                .expectNoEvent(Duration.ofMillis(500))
                .thenAwait(Duration.ofMillis(500))
                .expectNext(2L)
                .expectNoEvent(Duration.ofMillis(500))
                .thenAwait(Duration.ofMillis(500))
                .expectNext(3L)
                .verifyComplete();
    }

    @Test
    public void concatMap_givenHigherLimit_expectsMoreImmediateElements() {
        StepVerifier.withVirtualTime(() -> Flux.just(1L, 2L, 3L, 4L, 5L)
                        .transform(newConcatMap()
                                .limits(Mono.just(Rate.perSecond(4)))
                                .build()))
                .expectNext(1L, 2L, 3L, 4L)
                .expectNoEvent(Duration.ofMillis(200))
                .thenAwait(Duration.ofMillis(50))
                .expectNext(5L)
                .verifyComplete();
    }

    // ===== concatMap: Weighting scenarios =====

    @Test
    public void concatMap_givenWeighting_expectsProportionalDelay() {
        StepVerifier.withVirtualTime(() -> Flux.just(1L, 2L)
                        .transform(newConcatMap()
                                .limits(Mono.just(Rate.perSecond(2)))
                                .weighting(__ -> 2)
                                .build()))
                .expectNext(1L)
                .expectNoEvent(Duration.ofMillis(500))
                .thenAwait(Duration.ofMillis(500))
                .expectNext(2L)
                .verifyComplete();
    }

    // ===== concatMap: Dynamic limit change scenarios =====

    @Test
    public void concatMap_givenLimitChangesToZero_expectsElementsStop() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> elementSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> elementSink
                        .asFlux()
                        .transform(newConcatMap().limits(limitSink.asFlux()).build()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.infinite()))
                .then(() -> elementSink.tryEmitNext(1L))
                .expectNext(1L)
                .then(() -> limitSink.tryEmitNext(Rate.zero()))
                .then(() -> elementSink.tryEmitNext(2L))
                .expectNoEvent(Duration.ofSeconds(5))
                .thenCancel()
                .verify();
    }

    @Test
    public void concatMap_givenLimitChangesFromZeroToPositive_expectsElementsResume() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> elementSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> elementSink
                        .asFlux()
                        .transform(newConcatMap().limits(limitSink.asFlux()).build()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.zero()))
                .expectNoEvent(Duration.ofSeconds(5))
                .then(() -> limitSink.tryEmitNext(Rate.perSecond(100)))
                .then(() -> elementSink.tryEmitNext(1L))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    @Test
    public void concatMap_givenLimitErrors_expectsError() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> elementSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> elementSink
                        .asFlux()
                        .transform(newConcatMap().limits(limitSink.asFlux()).build()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.perSecond(10)))
                .then(() -> elementSink.tryEmitNext(1L))
                .expectNext(1L)
                .then(() -> limitSink.tryEmitError(new RuntimeException("limit error")))
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    public void concatMap_givenLimitChangesFromPositiveToInfinite_expectsNoDelay() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> elementSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> elementSink
                        .asFlux()
                        .transform(newConcatMap().limits(limitSink.asFlux()).build()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    elementSink.tryEmitNext(1L);
                    elementSink.tryEmitNext(2L);
                })
                .expectNext(1L)
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(2L)
                .then(() -> limitSink.tryEmitNext(Rate.infinite()))
                .then(() -> {
                    elementSink.tryEmitNext(3L);
                    elementSink.tryEmitNext(4L);
                })
                .expectNext(3L, 4L)
                .thenCancel()
                .verify();
    }

    @Test
    public void concatMap_givenLimitChangesFromInfiniteToPositive_expectsDelayedElements() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> elementSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> elementSink
                        .asFlux()
                        .transform(newConcatMap().limits(limitSink.asFlux()).build()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.infinite()))
                .then(() -> {
                    elementSink.tryEmitNext(1L);
                    elementSink.tryEmitNext(2L);
                })
                .expectNext(1L, 2L)
                .then(() -> limitSink.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    elementSink.tryEmitNext(3L);
                    elementSink.tryEmitNext(4L);
                })
                .expectNext(3L)
                .expectNoEvent(Duration.ofMillis(500))
                .thenAwait(Duration.ofMillis(500))
                .expectNext(4L)
                .thenCancel()
                .verify();
    }

    @Test
    public void concatMap_givenLimitIncreasesAtRuntime_expectsFasterThroughput() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> elementSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> elementSink
                        .asFlux()
                        .transform(newConcatMap().limits(limitSink.asFlux()).build()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.perSecond(1)))
                .then(() -> {
                    elementSink.tryEmitNext(1L);
                    elementSink.tryEmitNext(2L);
                })
                .expectNext(1L)
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(2L)
                .then(() -> limitSink.tryEmitNext(Rate.perSecond(100)))
                .then(() -> {
                    elementSink.tryEmitNext(3L);
                    elementSink.tryEmitNext(4L);
                    elementSink.tryEmitNext(5L);
                })
                .thenAwait(Duration.ofMillis(100))
                .expectNext(3L, 4L, 5L)
                .thenCancel()
                .verify();
    }

    // ===== innerConcatMap: Nuances vs concatMap =====

    @Test
    public void innerConcatMap_givenCompletedSource_expectsNoCompletion() {
        Flux<Flux<Long>> source = Flux.just(Flux.just(1L, 2L), Flux.just(3L, 4L));

        StepVerifier.withVirtualTime(() -> source.transform(newInnerConcatMap()
                                .limits(Mono.just(Rate.infinite()))
                                .build())
                        .flatMap(Function.identity()))
                .expectNext(1L, 2L, 3L, 4L)
                .expectNoEvent(Duration.ofSeconds(5))
                .thenCancel()
                .verify();
    }

    @Test
    public void innerConcatMap_givenZeroLimit_expectsInnersCancelled() {
        Sinks.Many<Rate> limitSink = Sinks.many().multicast().onBackpressureBuffer();
        Sinks.Many<Long> innerSink = Sinks.many().multicast().onBackpressureBuffer();

        StepVerifier.withVirtualTime(() -> Flux.just(innerSink.asFlux())
                        .transform(
                                newInnerConcatMap().limits(limitSink.asFlux()).build())
                        .flatMap(Function.identity()))
                .expectSubscription()
                .then(() -> limitSink.tryEmitNext(Rate.infinite()))
                .then(() -> innerSink.tryEmitNext(1L))
                .expectNext(1L)
                .then(() -> limitSink.tryEmitNext(Rate.zero()))
                .expectNoEvent(Duration.ofSeconds(5))
                .thenCancel()
                .verify();
    }

    // ===== Utility methods =====

    private static ThroughputLimitingTransformer.Builder<Long, Long> newConcatMap() {
        return ThroughputLimitingTransformer.concatMap(0);
    }

    private static ThroughputLimitingTransformer.Builder<Long, Flux<Long>> newInnerConcatMap() {
        return ThroughputLimitingTransformer.innerConcatMap(0);
    }
}
